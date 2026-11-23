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
    if trait_path.segments.last().is_none_or(|segment| segment.ident != "EventHandler") {
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
        Error::new(handle.sig.span(), "`handle` needs an `event: &EventEnvelope<Event>` argument")
    })?;
    let context = typed.next().ok_or_else(|| {
        Error::new(handle.sig.span(), "`handle` needs a `context: &mut ProcessingContext<'_, Tx>` argument")
    })?;
    let transaction = context_transaction(context)?;

    // No unit of work named: generic over any, so it runs with every token
    // store; the contexts written `ProcessingContext<'_>` get it too.
    let transaction = match transaction {
        Some(transaction) => transaction,
        None => {
            item.generics.params.push(GenericParam::Type(parse_quote!(__CqrsTx: ::core::marker::Send)));
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
        let self_ty = &item.self_ty;
        let mut generics = item.generics.clone();
        generics.params = generics
            .params
            .into_iter()
            .filter(|param| !matches!(param, GenericParam::Type(ty) if ty.ident == "__CqrsTx"))
            .collect();
        let (impl_generics, _, where_clause) = generics.split_for_impl();
        quote! {
            #[automatically_derived]
            impl #impl_generics #krate::Projection for #self_ty #where_clause {
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
    let Type::Reference(TypeReference { elem, mutability: None, .. }) = ty else {
        return Err(error());
    };
    let Type::Path(path) = &**elem else {
        return Err(error());
    };
    let segment = path.path.segments.last().filter(|segment| segment.ident == "EventEnvelope").ok_or_else(error)?;
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
    let error = || Error::new(ty.span(), "expected `&mut ProcessingContext<'_>` or `&mut ProcessingContext<'_, Tx>`");
    let Type::Reference(TypeReference { elem, mutability: Some(_), .. }) = ty else {
        return Err(error());
    };
    let Type::Path(path) = &**elem else {
        return Err(error());
    };
    let segment = path.path.segments.last().filter(|segment| segment.ident == "ProcessingContext").ok_or_else(error)?;
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
                    if !arguments.args.iter().any(|argument| matches!(argument, GenericArgument::Type(_))) =>
                {
                    arguments.args.push(parse_quote!(#transaction));
                }
                PathArguments::None => segment.arguments = PathArguments::AngleBracketed(parse_quote!(<'_, #transaction>)),
                _ => {}
            }
        }
        syn::visit_mut::visit_path_segment_mut(self, segment);
    }
}
