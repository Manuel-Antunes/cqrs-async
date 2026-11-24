//! `#[projection]` on `impl EventHandler for H`: fills in the event type and
//! the unit of work from `handle`'s signature, and names the processor.

use proc_macro2::TokenStream as TokenStream2;
use quote::{ToTokens, quote};
use syn::{
    Error, FnArg, GenericArgument, GenericParam, ImplItem, ItemImpl, LitStr, PathArguments, Result, Type,
    TypeReference, parse_quote, spanned::Spanned, visit_mut::VisitMut,
};

use crate::krate;

/// The arguments of `#[projection(...)]`.
#[derive(Default)]
pub struct Arguments {
    /// `namespace = "..."`: the processor's name.
    pub namespace: Option<LitStr>,
}

impl syn::parse::Parse for Arguments {
    fn parse(input: syn::parse::ParseStream) -> Result<Self> {
        let mut arguments = Self::default();
        if input.is_empty() {
            return Ok(arguments);
        }
        let key: syn::Ident = input.parse()?;
        if key != "namespace" {
            return Err(Error::new(key.span(), "expected `namespace = \"...\"`"));
        }
        input.parse::<syn::Token![=]>()?;
        arguments.namespace = Some(input.parse()?);
        Ok(arguments)
    }
}

/// Expansion of `#[projection(...)]`.
pub fn projection(arguments: Arguments, mut item: ItemImpl) -> Result<TokenStream2> {
    let krate = krate();
    let Some((_, trait_path, _)) = &item.trait_ else {
        return Err(Error::new(item.self_ty.span(), "expected `impl EventHandler for ...`"));
    };
    if trait_path
        .segments
        .last()
        .is_none_or(|segment| segment.ident != "EventHandler")
    {
        return Err(Error::new(trait_path.span(), "expected `impl EventHandler for ...`"));
    }

    let handle = item
        .items
        .iter()
        .find_map(|item| match item {
            ImplItem::Fn(method) if method.sig.ident == "handle" => Some(method),
            _ => None,
        })
        .ok_or_else(|| Error::new(item.self_ty.span(), "expected a `handle` method in this impl"))?;
    let mut typed = handle.sig.inputs.iter().filter_map(|input| match input {
        FnArg::Typed(argument) => Some(&*argument.ty),
        FnArg::Receiver(_) => None,
    });
    let event = typed.next().map(envelope_event).transpose()?.flatten().ok_or_else(|| {
        Error::new(
            handle.sig.span(),
            "`handle` needs an `event: &EventEnvelope<Event>` argument",
        )
    })?;
    let context = typed.next().ok_or_else(|| {
        Error::new(
            handle.sig.span(),
            "`handle` needs a `context: &mut ProcessingContext<'_, Tx>` argument",
        )
    })?;
    let transaction = context_transaction(context)?;

    // No unit of work named: generic over any, so it runs with every token
    // store; the contexts written `ProcessingContext<'_>` get it too.
    let transaction = match transaction {
        Some(transaction) => transaction,
        None => {
            item.generics
                .params
                .push(GenericParam::Type(parse_quote!(__CqrsTx: ::core::marker::Send)));
            let generic: Type = parse_quote!(__CqrsTx);
            for item in &mut item.items {
                if let ImplItem::Fn(method) = item {
                    AddTransaction(&generic).visit_signature_mut(&mut method.sig);
                }
            }
            generic
        }
    };
    let (_, trait_path, _) = item.trait_.as_mut().expect("checked above");
    let last = trait_path.segments.last_mut().expect("checked above");
    last.arguments = PathArguments::AngleBracketed(parse_quote!(<#event, #transaction>));

    let named = arguments.namespace.map(|namespace| {
        // The handler's type alone has a namespace, whatever the unit of
        // work: only the parameters of the type, without the bounds the
        // handler needs (which may name the transaction).
        let self_ty = &item.self_ty;
        let used = idents(self_ty.to_token_stream());
        let mut generics = item.generics.clone();
        generics.where_clause = None;
        generics.params = generics
            .params
            .into_iter()
            .filter_map(|param| match param {
                GenericParam::Type(mut ty) => used.contains(&ty.ident.to_string()).then(|| {
                    ty.bounds.clear();
                    ty.colon_token = None;
                    ty.default = None;
                    GenericParam::Type(ty)
                }),
                GenericParam::Lifetime(mut lifetime) => used.contains(&lifetime.lifetime.ident.to_string()).then(|| {
                    lifetime.bounds.clear();
                    lifetime.colon_token = None;
                    GenericParam::Lifetime(lifetime)
                }),
                GenericParam::Const(constant) => {
                    used.contains(&constant.ident.to_string()).then_some(GenericParam::Const(constant))
                }
            })
            .collect();
        let (impl_generics, _, _) = generics.split_for_impl();
        quote! {
            #[automatically_derived]
            impl #impl_generics #krate::Projection for #self_ty where #self_ty: ::core::marker::Send + ::core::marker::Sync {
                const NAMESPACE: &'static str = #namespace;
            }
        }
    });
    let mut output = item.into_token_stream();
    output.extend(named);
    Ok(output)
}

/// `E` in `&EventEnvelope<E>`.
fn envelope_event(ty: &Type) -> Result<Option<Type>> {
    let error = || Error::new(ty.span(), "expected `&EventEnvelope<Event>`");
    let Type::Reference(TypeReference {
        elem, mutability: None, ..
    }) = ty
    else {
        return Err(error());
    };
    let Type::Path(path) = &**elem else {
        return Err(error());
    };
    let segment = path
        .path
        .segments
        .last()
        .filter(|segment| segment.ident == "EventEnvelope")
        .ok_or_else(error)?;
    let PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        return Err(error());
    };
    match arguments.args.first() {
        Some(GenericArgument::Type(event)) if arguments.args.len() == 1 => Ok(Some(event.clone())),
        _ => Err(error()),
    }
}

/// `Tx` in `&mut ProcessingContext<'_, Tx>`, or `None` for
/// `&mut ProcessingContext<'_>`.
fn context_transaction(ty: &Type) -> Result<Option<Type>> {
    let error = || {
        Error::new(
            ty.span(),
            "expected `&mut ProcessingContext<'_>` or `&mut ProcessingContext<'_, Tx>`",
        )
    };
    let Type::Reference(TypeReference {
        elem,
        mutability: Some(_),
        ..
    }) = ty
    else {
        return Err(error());
    };
    let Type::Path(path) = &**elem else {
        return Err(error());
    };
    let segment = path
        .path
        .segments
        .last()
        .filter(|segment| segment.ident == "ProcessingContext")
        .ok_or_else(error)?;
    let PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        return Ok(None);
    };
    Ok(arguments.args.iter().find_map(|argument| match argument {
        GenericArgument::Type(transaction) => Some(transaction.clone()),
        _ => None,
    }))
}

/// Turns every `ProcessingContext<'_>` of a signature into
/// `ProcessingContext<'_, Tx>`.
struct AddTransaction<'a>(&'a Type);

impl VisitMut for AddTransaction<'_> {
    fn visit_path_segment_mut(&mut self, segment: &mut syn::PathSegment) {
        if segment.ident == "ProcessingContext" {
            let transaction = self.0;
            match &mut segment.arguments {
                PathArguments::AngleBracketed(arguments)
                    if !arguments
                        .args
                        .iter()
                        .any(|argument| matches!(argument, GenericArgument::Type(_))) =>
                {
                    arguments.args.push(parse_quote!(#transaction));
                }
                PathArguments::None => {
                    segment.arguments = PathArguments::AngleBracketed(parse_quote!(<'_, #transaction>))
                }
                _ => {}
            }
        }
        syn::visit_mut::visit_path_segment_mut(self, segment);
    }
}

/// Every identifier in `tokens`, nested groups included.
fn idents(tokens: TokenStream2) -> std::collections::HashSet<String> {
    let mut found = std::collections::HashSet::new();
    for token in tokens {
        match token {
            proc_macro2::TokenTree::Ident(ident) => {
                found.insert(ident.to_string());
            }
            proc_macro2::TokenTree::Group(group) => found.extend(idents(group.stream())),
            proc_macro2::TokenTree::Punct(_) | proc_macro2::TokenTree::Literal(_) => {}
        }
    }
    found
}
