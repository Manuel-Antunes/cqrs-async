//! `#[command_handler]` and `#[query_handler]`: fill in the associated
//! types of a handler impl from the signature of its `handle`.

use proc_macro2::TokenStream as TokenStream2;
use quote::ToTokens;
use syn::{
    Error, FnArg, GenericArgument, ImplItem, ImplItemFn, ItemImpl, PathArguments, Result, Type, TypeReference,
    parse_quote, spanned::Spanned,
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

/// `#[command_handler]` on `impl CommandHandler for H`: the command is the
/// type behind `handle`'s `&Command` argument, the event the parameter of
/// its `&mut Appender<Event>` argument, and the unit of work its third
/// argument: `tx: &mut Tx` is `InTransaction<Tx>`; without one, `Autocommit`
/// (and the macro adds the `&mut ()` the trait passes). It adds the three
/// associated types.
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
    let appender = argument(method, 1, "an `events: &mut Appender<Event>` argument")?;
    let event = appender_event(&appender)?;
    let transaction = match argument(method, 2, "") {
        Ok(Type::Reference(TypeReference {
            elem,
            mutability: Some(_),
            ..
        })) => Some(*elem),
        Ok(other) => {
            return Err(Error::new(
                other.span(),
                "expected `tx: &mut Tx`: the unit of work's transaction, lent to the handler",
            ));
        }
        Err(_) => None,
    };
    let unit: Type = match &transaction {
        Some(transaction) => parse_quote!(#krate::InTransaction<#transaction>),
        None => parse_quote!(#krate::Autocommit),
    };
    if transaction.is_none() {
        // The trait passes the unit's transaction, `()` here.
        let method = item
            .items
            .iter_mut()
            .find_map(|item| match item {
                ImplItem::Fn(method) if method.sig.ident == "handle" => Some(method),
                _ => None,
            })
            .expect("found above");
        method.sig.inputs.push(parse_quote!(_: &mut ()));
    }
    if !declares(&item, "Unit") {
        item.items.insert(0, parse_quote!(type Unit = #unit;));
    }
    if !declares(&item, "Event") {
        item.items.insert(0, parse_quote!(type Event = #event;));
    }
    if !declares(&item, "Command") {
        item.items.insert(0, parse_quote!(type Command = #command;));
    }
    Ok(item.into_token_stream())
}

/// `E` in `&mut Appender<E>`.
fn appender_event(ty: &Type) -> Result<Type> {
    let error = || Error::new(ty.span(), "expected `&mut Appender<Event>`");
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
        .filter(|segment| segment.ident == "Appender")
        .ok_or_else(error)?;
    let PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        return Err(error());
    };
    let mut types = arguments.args.iter().filter_map(|argument| match argument {
        GenericArgument::Type(ty) => Some(ty.clone()),
        _ => None,
    });
    match (types.next(), types.next()) {
        (Some(event), None) => Ok(event),
        _ => Err(error()),
    }
}

/// `#[query_handler]` on `impl Handler for H`: the message is the type of
/// `handle`'s argument.
pub fn query_handler(mut item: ItemImpl) -> Result<TokenStream2> {
    let method = handle(&item)?;
    let message = argument(method, 0, "a `query: Query` argument")?;
    if let Type::Reference(reference) = &message {
        return Err(Error::new(reference.span(), "expected the query by value"));
    }
    if !declares(&item, "Message") {
        item.items.insert(0, parse_quote!(type Message = #message;));
    }
    Ok(item.into_token_stream())
}
