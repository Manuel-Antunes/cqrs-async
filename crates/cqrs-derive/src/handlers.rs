//! `#[command_handler]` and `#[query_handler]`: fill in a handler impl's
//! message and associated types from the signature of its `handle`, and mark
//! the handler for the gateways that list its message.

use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{
    Error, FnArg, GenericArgument, GenericParam, ImplItem, ImplItemFn, ItemImpl, PathArguments, Result, Type,
    TypeReference, parse_quote, spanned::Spanned,
};

/// The `handle` method of `item`.
fn handle(item: &ItemImpl) -> Result<&ImplItemFn> {
    item.items
        .iter()
        .find_map(|item| match item {
            ImplItem::Fn(method) if method.sig.ident == "handle" => Some(method),
            _ => None,
        })
        .ok_or_else(|| Error::new(item.self_ty.span(), "expected a `handle` method in this impl"))
}

/// The type of the `index`-th argument after `&self`.
fn argument(method: &ImplItemFn, index: usize, expected: &str) -> Result<Type> {
    match method
        .sig
        .inputs
        .iter()
        .filter(|input| matches!(input, FnArg::Typed(_)))
        .nth(index)
    {
        Some(FnArg::Typed(argument)) => Ok((*argument.ty).clone()),
        _ => Err(Error::new(method.sig.span(), format!("`handle` needs {expected}"))),
    }
}

/// Whether `item` already declares the associated type `name`.
fn declares(item: &ItemImpl, name: &str) -> bool {
    item.items
        .iter()
        .any(|item| matches!(item, ImplItem::Type(ty) if ty.ident == name))
}

/// `#[command_handler]` on `impl CommandHandler<C> for H`: the command is the
/// type behind `handle`'s `&Command` argument (and the trait's parameter, if
/// left out), the event the first type argument of its
/// `&mut CommandContext<Event, ..>` argument, and the unit of work the
/// number of them: `CommandContext<Event>` is `Autocommit`;
/// `CommandContext<Event, Tx>` is `InTransaction<Tx>` (even for `()`). It adds
/// the two associated types, and the `Handles<C>` marker a gateway listing
/// `C` requires.
pub fn command_handler(mut item: ItemImpl) -> Result<TokenStream2> {
    let krate = crate::krate();
    let method = handle(&item)?;
    let command = match argument(method, 0, "a `&Command` argument")? {
        Type::Reference(TypeReference {
            elem, mutability: None, ..
        }) => *elem,
        other => {
            return Err(Error::new(
                other.span(),
                "expected `&Command`: the handler may run again after a lost race",
            ));
        }
    };
    let context = argument(method, 1, "a `ctx: &mut CommandContext<Event>` argument")?;
    let (event, transaction) = context_types(&context)?;
    let extra = method
        .sig
        .inputs
        .iter()
        .filter(|input| matches!(input, FnArg::Typed(_)))
        .nth(2);
    if let Some(extra) = extra {
        return Err(Error::new(
            extra.span(),
            "the unit's transaction is in the context: take `ctx: &mut CommandContext<Event, Tx>` and use \
             `ctx.tx()`",
        ));
    }
    let unit: Type = match &transaction {
        Some(transaction) => parse_quote!(#krate::InTransaction<#transaction>),
        None => parse_quote!(#krate::Autocommit),
    };
    if !declares(&item, "Unit") {
        item.items.insert(0, parse_quote!(type Unit = #unit;));
    }
    if !declares(&item, "Event") {
        item.items.insert(0, parse_quote!(type Event = #event;));
    }
    let command = message_parameter(&mut item, command, "CommandHandler")?;
    let marker = handles_marker(&item, &command);
    shared(&mut item);
    Ok(quote!(#item #marker))
}

/// The message of `impl Trait<M> for H`: `M` if written, else `inferred`
/// (from `handle`'s signature), which is then written in.
fn message_parameter(item: &mut ItemImpl, inferred: Type, expected: &str) -> Result<Type> {
    let Some((_, path, _)) = &mut item.trait_ else {
        return Err(Error::new(
            item.self_ty.span(),
            format!("expected `impl {expected}<Message> for ...`"),
        ));
    };
    let segment = path.segments.last_mut().ok_or_else(|| {
        Error::new(
            item.self_ty.span(),
            format!("expected `impl {expected}<Message> for ...`"),
        )
    })?;
    match &segment.arguments {
        PathArguments::None => {
            segment.arguments = PathArguments::AngleBracketed(parse_quote!(<#inferred>));
            Ok(inferred)
        }
        PathArguments::AngleBracketed(arguments) => {
            let mut types = arguments.args.iter().filter_map(|argument| match argument {
                GenericArgument::Type(ty) => Some(ty.clone()),
                _ => None,
            });
            match (types.next(), types.next()) {
                (Some(message), None) => Ok(message),
                _ => Err(Error::new(segment.span(), format!("expected `{expected}<Message>`"))),
            }
        }
        PathArguments::Parenthesized(_) => Err(Error::new(segment.span(), format!("expected `{expected}<Message>`"))),
    }
}

/// Bounds a generic handler impl by `H: Send + Sync`, which the handler
/// traits require: the gateway's parameters (its unit of work, its
/// repository) then need no `Send + Sync` of their own in every impl.
fn shared(item: &mut ItemImpl) {
    if item.generics.params.is_empty() {
        return;
    }
    let self_ty = &item.self_ty;
    let predicate: syn::WherePredicate = parse_quote!(#self_ty: ::core::marker::Send + ::core::marker::Sync);
    item.generics.make_where_clause().predicates.push(predicate);
}

/// `impl<..> Handles<M> for H {}` with the impl's parameters that `H` or `M`
/// use, without their bounds: true whatever bounds the handler impl needs,
/// so a gateway can require it where it is declared.
fn handles_marker(item: &ItemImpl, message: &Type) -> TokenStream2 {
    let krate = crate::krate();
    let self_ty = &item.self_ty;
    let used = quote!(#self_ty #message).to_string();
    let used: Vec<String> = used
        .split(|c: char| !(c.is_alphanumeric() || c == '_' || c == '\''))
        .map(str::to_owned)
        .collect();
    let params = item.generics.params.iter().filter_map(|param| match param {
        GenericParam::Type(param) => used.contains(&param.ident.to_string()).then(|| {
            let ident = &param.ident;
            quote!(#ident)
        }),
        GenericParam::Lifetime(param) => used.contains(&param.lifetime.to_string()).then(|| {
            let lifetime = &param.lifetime;
            quote!(#lifetime)
        }),
        GenericParam::Const(param) => used.contains(&param.ident.to_string()).then(|| {
            let (ident, ty) = (&param.ident, &param.ty);
            quote!(const #ident: #ty)
        }),
    });
    quote! {
        #[automatically_derived]
        impl<#(#params),*> #krate::Handles<#message> for #self_ty {}
    }
}

/// `E` in `&mut CommandContext<E>`, and `Tx` in `&mut CommandContext<E, Tx>`
/// (`None` when it is not written).
fn context_types(ty: &Type) -> Result<(Type, Option<Type>)> {
    let error = || {
        Error::new(
            ty.span(),
            "expected `&mut CommandContext<Event>` or `&mut CommandContext<Event, Tx>`",
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
        .filter(|segment| segment.ident == "CommandContext")
        .ok_or_else(error)?;
    let PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        return Err(error());
    };
    let mut types = arguments.args.iter().filter_map(|argument| match argument {
        GenericArgument::Type(ty) => Some(ty.clone()),
        _ => None,
    });
    match (types.next(), types.next(), types.next()) {
        (Some(event), transaction, None) => Ok((event, transaction)),
        _ => Err(error()),
    }
}

/// `#[query_handler]` on `impl QueryHandler<Q> for H`: the query is the type
/// of `handle`'s argument (and the trait's parameter, if left out). It adds
/// the `Handles<Q>` marker a gateway listing `Q` requires.
pub fn query_handler(mut item: ItemImpl) -> Result<TokenStream2> {
    let method = handle(&item)?;
    let message = argument(method, 0, "a `query: Query` argument")?;
    if let Type::Reference(reference) = &message {
        return Err(Error::new(reference.span(), "expected the query by value"));
    }
    let message = message_parameter(&mut item, message, "QueryHandler")?;
    let marker = handles_marker(&item, &message);
    shared(&mut item);
    Ok(quote!(#item #marker))
}
