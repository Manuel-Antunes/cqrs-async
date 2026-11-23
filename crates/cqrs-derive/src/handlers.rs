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
/// its `&mut Appender<Event>` argument. The publisher is the gateway's, so
/// `handle` is generic over it: `Appender<Event>` becomes
/// `Appender<Event, P>` with `P: EventPublisher<Event>` added to `handle`
/// (an appender written with its publisher is kept as is).
pub fn command_handler(mut item: ItemImpl) -> Result<TokenStream2> {
    let krate = crate::krate();
    let method = handle_mut(&mut item)?;
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
    let (event, publisher) = appender_arguments(&appender)?;
    if publisher.is_none() {
        // `Appender<Event>` → `Appender<Event, __P>`, `handle<__P: EventPublisher<Event>>`.
        method
            .sig
            .generics
            .params
            .push(parse_quote!(__P: #krate::EventPublisher<#event>));
        // The path as written (so its import stays used), one argument more.
        if let Some(FnArg::Typed(argument)) = method
            .sig
            .inputs
            .iter_mut()
            .filter(|input| matches!(input, FnArg::Typed(_)))
            .nth(1)
            && let Type::Reference(reference) = &mut *argument.ty
            && let Type::Path(path) = &mut *reference.elem
            && let Some(segment) = path.path.segments.last_mut()
            && let PathArguments::AngleBracketed(arguments) = &mut segment.arguments
        {
            arguments.args.push(parse_quote!(__P));
        }
    }
    if !declares(&item, "Event") {
        item.items.insert(0, parse_quote!(type Event = #event;));
    }
    if !declares(&item, "Command") {
        item.items.insert(0, parse_quote!(type Command = #command;));
    }
    Ok(item.into_token_stream())
}

/// The `handle` method of `item`, to rewrite.
fn handle_mut(item: &mut ItemImpl) -> Result<&mut ImplItemFn> {
    let span = item.self_ty.span();
    item.items
        .iter_mut()
        .find_map(|item| match item {
            ImplItem::Fn(method) if method.sig.ident == "handle" => Some(method),
            _ => None,
        })
        .ok_or_else(|| Error::new(span, "expected a `handle` method in this impl"))
}

/// `E` (and `P`, if written) in `&mut Appender<E>` / `&mut Appender<E, P>`.
fn appender_arguments(ty: &Type) -> Result<(Type, Option<Type>)> {
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
    match (types.next(), types.next(), types.next()) {
        (Some(event), publisher, None) => Ok((event, publisher)),
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
