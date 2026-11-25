//! Macros for the `cqrs` crate. Each one only writes what the user could
//! write by hand (trait impls, or the associated types of an impl); none of
//! them adds fields or rewrites a type.

mod aggregate_root;
mod gateway;
mod handlers;
mod projection;
mod query_updates;

use proc_macro::TokenStream;
use proc_macro_crate::{FoundCrate, crate_name};
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::{ToTokens, quote};
use syn::{Data, DeriveInput, Error, Ident, LitInt, LitStr, Result, Type, parse_macro_input, spanned::Spanned};

/// Path to what the macros name in the `cqrs` crate (its `__private`
/// module, the same whatever public module an item lives in), even when a
/// consumer renamed the dependency.
fn krate() -> TokenStream2 {
    match crate_name("cqrs") {
        Ok(FoundCrate::Name(name)) => {
            let ident = Ident::new(&name, Span::call_site());
            quote!(::#ident::__private)
        }
        // `cqrs` itself declares `extern crate self as cqrs`.
        Ok(FoundCrate::Itself) | Err(_) => quote!(::cqrs::__private),
    }
}

/// On a struct that handles commands, one `CommandHandler<C>` impl each:
/// `#[cqrs::command_gateway(handles(CreatePost, PublishPost))]` makes it
/// their gateway. Each command listed is routed to its impl
/// (`Dispatch<C>`), in a unit of work of the field marked
/// `#[gateway(unit)]`; a command listed without its impl
/// (`#[cqrs::command_handler]`) does not compile.
///
/// `commands = Name` also generates a trait bounded by `CommandGateway` of
/// every command listed, implemented by every gateway that routes them:
/// what code depending on the commands names (`impl PostCommands`), whatever
/// gateway, interceptors or unit stand behind it.
///
/// ```ignore
/// #[cqrs::command_gateway(commands = PostCommands, handles(CreatePost, PublishPost))]
/// pub struct PostCommandGateway<R, P> {
///     posts: R,
///     #[gateway(unit)]
///     events: P,
/// }
///
/// #[cqrs::command_handler]
/// impl<R: PostRepository, P> CommandHandler<CreatePost> for PostCommandGateway<R, P> { .. }
/// ```
#[proc_macro_attribute]
pub fn command_gateway(arguments: TokenStream, item: TokenStream) -> TokenStream {
    let parser = |input: syn::parse::ParseStream| gateway::GatewayArguments::parse(gateway::Kind::Commands, input);
    let arguments = parse_macro_input!(arguments with parser);
    into_stream(gateway::gateway(
        gateway::Kind::Commands,
        arguments,
        parse_macro_input!(item),
    ))
}

/// The query counterpart of `#[command_gateway]`: `handles(Q, ...)`, each
/// routed to the struct's `QueryHandler<Q>` impl. `queries = Name` generates
/// the trait of every gateway answering them. Subscribing is the
/// projection's (`#[cqrs::projection(.., updates(..), subscriptions =
/// Name)]`): it pushes the updates.
#[proc_macro_attribute]
pub fn query_gateway(arguments: TokenStream, item: TokenStream) -> TokenStream {
    let parser = |input: syn::parse::ParseStream| gateway::GatewayArguments::parse(gateway::Kind::Queries, input);
    let arguments = parse_macro_input!(arguments with parser);
    into_stream(gateway::gateway(
        gateway::Kind::Queries,
        arguments,
        parse_macro_input!(item),
    ))
}

/// A struct of `QueryUpdateChannel<Q>` fields says where each query's
/// updates are emitted: it implements `UpdatesOf<Q>` per field, so
/// `SubscriptionQueries` can subscribe to any of them. For updates
/// assembled by hand; a projection's `updates(..)` makes its own.
///
/// ```ignore
/// #[derive(Default, QueryUpdates)]
/// pub struct PostUpdates {
///     pub find: QueryUpdateChannel<FindPost>,
///     pub published: QueryUpdateChannel<ListPublished>,
/// }
/// ```
#[proc_macro_derive(QueryUpdates)]
pub fn derive_query_updates(input: TokenStream) -> TokenStream {
    into_stream(query_updates::query_updates(parse_macro_input!(input)))
}

/// On `impl EventHandler for H`: fills in the event type and the unit of
/// work from `handle`'s signature (`&EventEnvelope<Event>`,
/// `&mut ProcessingContext<'_, Tx>`; without a `Tx`, generic over any).
///
/// With `namespace = "..."`, the impl is the handler of that namespace of
/// `H`'s projection (`#[cqrs::projection]` on `H`, which must list it). One
/// struct then has one such impl per namespace, each run by a processor of
/// its own.
///
/// ```ignore
/// #[cqrs::event_handler(namespace = "summaries")]
/// impl EventHandler for PostProjection {
///     async fn handle(&self, event: &EventEnvelope<PostEvent>, context: &mut ProcessingContext<'_>)
///         -> Result<(), HandlerError> { .. }
/// }
/// ```
#[proc_macro_attribute]
pub fn event_handler(arguments: TokenStream, item: TokenStream) -> TokenStream {
    into_stream(projection::event_handler(
        parse_macro_input!(arguments),
        parse_macro_input!(item),
    ))
}

/// On a projection's struct: `#[cqrs::projection(name = "post", events =
/// PostEvent, namespaces("summaries", "notifications"))]`. Implements
/// `Projection` (its name, events and namespaces) and `Route` (each
/// namespace to its `#[cqrs::event_handler(namespace = ..)]` impl): a
/// projection missing a handler does not run, and a handler for a namespace
/// not listed does not compile. Put it before `#[derive]`.
///
/// `updates(FindPost, ..)` lists its subscription queries: it gets a channel
/// for each (`QueryUpdateEmitter<Self>`), which a namespace's
/// `async fn updated(&self, event, updates: &QueryUpdateEmitter<Self>)`
/// emits through, and which the unit's subscriptions listen to.
/// `subscriptions = Name` also generates the trait of whatever subscribes to
/// them all.
#[proc_macro_attribute]
pub fn projection(arguments: TokenStream, item: TokenStream) -> TokenStream {
    into_stream(projection::projection(
        parse_macro_input!(arguments),
        parse_macro_input!(item),
    ))
}

#[proc_macro_derive(AggregateRoot, attributes(aggregate))]
pub fn derive_aggregate_root(input: TokenStream) -> TokenStream {
    into_stream(aggregate_root::aggregate_root(parse_macro_input!(input)))
}

/// On `impl CommandHandler<C> for H`: adds `type Event` and `type Unit`,
/// read from `handle(&self, command: &C, events: &mut CommandContext<Event>[, tx:
/// &mut Tx])`, and `C` if the trait leaves it out. Types written by hand are
/// kept. It also marks `H` as handling `C`, for the gateway that lists it.
///
/// ```ignore
/// #[command_handler]
/// impl<R: PostRepository, P> CommandHandler<PublishPost> for PostCommandGateway<R, P> {
///     async fn handle(&self, command: &PublishPost, events: &mut CommandContext<PostEvent>, tx: &mut R::Tx)
///         -> Result<(), BlogError> { ... }
/// }
/// ```
#[proc_macro_attribute]
pub fn command_handler(arguments: TokenStream, item: TokenStream) -> TokenStream {
    if !arguments.is_empty() {
        return Error::new(Span::call_site(), "`#[command_handler]` takes no arguments")
            .into_compile_error()
            .into();
    }
    into_stream(handlers::command_handler(parse_macro_input!(item)))
}

/// On `impl QueryHandler<Q> for H`: adds `Q` if the trait leaves it out,
/// read from `handle(&self, query: Q)`, and marks `H` as answering `Q`, for
/// the gateway that lists it.
#[proc_macro_attribute]
pub fn query_handler(arguments: TokenStream, item: TokenStream) -> TokenStream {
    if !arguments.is_empty() {
        return Error::new(Span::call_site(), "`#[query_handler]` takes no arguments")
            .into_compile_error()
            .into();
    }
    into_stream(handlers::query_handler(parse_macro_input!(item)))
}

/// Turns an expansion error into a `compile_error!` spanned on the offending
/// tokens, so the user sees it in place instead of a macro panic.
fn into_stream(result: Result<TokenStream2>) -> TokenStream {
    result.unwrap_or_else(Error::into_compile_error).into()
}

// ---------------------------------------------------------------------------
// #[derive(DomainEvent)]
// ---------------------------------------------------------------------------

/// Implements `DomainEvent`. The event type is the variant name (or the
/// struct name), overridable with `#[event(name = "...")]`. The schema
/// version is `#[event(version = N)]` on the variant, else on the
/// container, else 1: each variant is its own event type, versioned on its
/// own.
///
/// Give events explicit names when they are stored: the default follows the
/// Rust name, so renaming a variant would otherwise break old events.
///
/// It implements the event's tags too (`TagResolver`, which every
/// `DomainEvent` has), from the fields marked `#[tag]` (Axon's `@EventTag`):
/// each becomes a tag keyed by the field name, valued by its `Display`;
/// `#[tag = "key"]` names the key, and is required on tuple fields. Without
/// any, the event has no tags. To resolve them by hand, implement both
/// traits instead of deriving.
#[proc_macro_derive(DomainEvent, attributes(event, tag))]
pub fn derive_domain_event(input: TokenStream) -> TokenStream {
    into_stream(domain_event(parse_macro_input!(input)))
}

/// The arguments of one or more `#[event(...)]` attributes.
#[derive(Default)]
struct EventAttrs {
    /// `name = "..."`: the stored event type.
    name: Option<LitStr>,
    /// `version = N`: the payload's schema version.
    version: Option<LitInt>,
}

/// Parses the `#[event(...)]` attributes among `attrs`, rejecting unknown
/// keys.
fn event_attrs(attrs: &[syn::Attribute]) -> Result<EventAttrs> {
    let mut parsed = EventAttrs::default();
    for attr in attrs.iter().filter(|attr| attr.path().is_ident("event")) {
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("name") {
                parsed.name = Some(meta.value()?.parse()?);
            } else if meta.path.is_ident("version") {
                parsed.version = Some(meta.value()?.parse()?);
            } else {
                return Err(meta.error("expected `name = \"...\"` or `version = N`"));
            }
            Ok(())
        })?;
    }
    Ok(parsed)
}

/// Expansion of `#[derive(DomainEvent)]`: an `event_type` that matches on
/// the variant (or returns the struct's name) and, if given, `event_version`.
fn domain_event(input: DeriveInput) -> Result<TokenStream2> {
    let krate = krate();
    let name = &input.ident;
    let container = event_attrs(&input.attrs)?;
    if let Some(name) = &container.name
        && matches!(input.data, Data::Enum(_))
    {
        return Err(Error::new_spanned(
            name,
            "on an enum, put `#[event(name = ...)]` on the variants",
        ));
    }

    let (event_type, version) = match &input.data {
        Data::Enum(data) => {
            let mut types = Vec::new();
            let mut versions = Vec::new();
            let mut any_version = container.version.is_some();
            for variant in &data.variants {
                let ident = &variant.ident;
                let attrs = event_attrs(&variant.attrs)?;
                let label = attrs
                    .name
                    .unwrap_or_else(|| LitStr::new(&ident.to_string(), ident.span()));
                types.push(quote!(Self::#ident { .. } => #label));
                any_version |= attrs.version.is_some();
                let version = attrs.version.or_else(|| container.version.clone());
                let version = version.map_or_else(|| quote!(1), |version| quote!(#version));
                versions.push(quote!(Self::#ident { .. } => #version));
            }
            if types.is_empty() {
                (quote!(match *self {}), None)
            } else {
                let version = any_version.then(|| quote!(match self { #(#versions,)* }));
                (quote!(match self { #(#types,)* }), version)
            }
        }
        Data::Struct(_) => {
            let label = container
                .name
                .clone()
                .unwrap_or_else(|| LitStr::new(&name.to_string(), name.span()));
            (
                quote!(#label),
                container.version.as_ref().map(|version| quote!(#version)),
            )
        }
        Data::Union(_) => {
            return Err(Error::new_spanned(name, "events cannot be unions"));
        }
    };
    let version = version.map(|version| {
        quote! {
            fn event_version(&self) -> u32 {
                #version
            }
        }
    });

    let tags = tag_resolver(&input)?;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    Ok(quote! {
        #[automatically_derived]
        impl #impl_generics #krate::DomainEvent for #name #ty_generics #where_clause {
            fn event_type(&self) -> &'static str {
                #event_type
            }

            #version
        }

        #tags
    })
}

/// The `TagResolver` of a derived event: a `fn tags` over the `#[tag]`
/// fields, or the trait's default (no tags) without any.
fn tag_resolver(input: &DeriveInput) -> Result<TokenStream2> {
    let krate = krate();
    let name = &input.ident;
    let tags = event_tags(&krate, input)?;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    Ok(quote! {
        #[automatically_derived]
        impl #impl_generics #krate::TagResolver for #name #ty_generics #where_clause {
            #tags
        }
    })
}

/// The `fn tags` of the derived `TagResolver`, if any field is `#[tag]`ged
/// (else the trait's default: none).
fn event_tags(krate: &TokenStream2, input: &DeriveInput) -> Result<Option<TokenStream2>> {
    let mut arms = Vec::new();
    match &input.data {
        Data::Enum(data) => {
            for variant in &data.variants {
                let ident = &variant.ident;
                if let Some((pattern, tags)) = tagged_fields(krate, &variant.fields)? {
                    arms.push(quote!(Self::#ident #pattern => ::std::vec![#(#tags),*]));
                }
            }
        }
        Data::Struct(data) => {
            if let Some((pattern, tags)) = tagged_fields(krate, &data.fields)? {
                arms.push(quote!(Self #pattern => ::std::vec![#(#tags),*]));
            }
        }
        Data::Union(_) => {}
    }
    if arms.is_empty() {
        return Ok(None);
    }
    Ok(Some(quote! {
        fn tags(&self) -> ::std::vec::Vec<#krate::Tag> {
            #[allow(unreachable_patterns)]
            match self {
                #(#arms,)*
                _ => ::std::vec::Vec::new(),
            }
        }
    }))
}

/// A pattern binding the `#[tag]` fields of `fields`, and the `Tag` of
/// each; `None` without tagged fields.
fn tagged_fields(krate: &TokenStream2, fields: &syn::Fields) -> Result<Option<(TokenStream2, Vec<TokenStream2>)>> {
    let mut bindings = Vec::new();
    let mut tags = Vec::new();
    for (index, field) in fields.iter().enumerate() {
        let Some(attr) = field.attrs.iter().find(|attr| attr.path().is_ident("tag")) else {
            bindings.push(None);
            continue;
        };
        let key = match &attr.meta {
            syn::Meta::Path(_) => match &field.ident {
                Some(ident) => LitStr::new(&ident.to_string(), ident.span()),
                None => {
                    return Err(Error::new_spanned(
                        attr,
                        "a tuple field needs a key: `#[tag = \"key\"]`",
                    ));
                }
            },
            syn::Meta::NameValue(value) => syn::parse2(value.value.to_token_stream())?,
            syn::Meta::List(_) => return Err(Error::new_spanned(attr, "expected `#[tag]` or `#[tag = \"key\"]`")),
        };
        let binding = Ident::new(&format!("__tag_{index}"), field.span());
        tags.push(quote!(#krate::Tag::new(#key, #binding)));
        bindings.push(Some(binding));
    }
    if tags.is_empty() {
        return Ok(None);
    }
    let pattern = match fields {
        syn::Fields::Named(named) => {
            let parts = named.named.iter().zip(&bindings).filter_map(|(field, binding)| {
                let ident = field.ident.as_ref()?;
                binding.as_ref().map(|binding| quote!(#ident: #binding))
            });
            quote!({ #(#parts,)* .. })
        }
        syn::Fields::Unnamed(_) => {
            let parts = bindings
                .iter()
                .map(|binding| binding.as_ref().map_or_else(|| quote!(_), |binding| quote!(#binding)));
            quote!((#(#parts),*))
        }
        syn::Fields::Unit => return Ok(None),
    };
    Ok(Some((pattern, tags)))
}

// ---------------------------------------------------------------------------
// #[aggregate(...)] / #[command(...)] / #[query(...)]: the derives, as one
// attribute
// ---------------------------------------------------------------------------

/// Declares an aggregate root in one attribute:
/// `#[cqrs::aggregate(event = PostEvent)]` is `#[derive(AggregateRoot)]`
/// with `#[aggregate(event = PostEvent)]` (same arguments, and
/// `#[aggregate(id)]` on a field still marks the id).
#[proc_macro_attribute]
pub fn aggregate(arguments: TokenStream, item: TokenStream) -> TokenStream {
    into_stream(attribute_form(
        arguments.into(),
        item.into(),
        "aggregate",
        aggregate_root::aggregate_root,
    ))
}

/// Declares a command in one attribute: `#[cqrs::command(output = T, error =
/// E)]` is `#[derive(Command)]` with `#[command(...)]`.
#[proc_macro_attribute]
pub fn command(arguments: TokenStream, item: TokenStream) -> TokenStream {
    into_stream(attribute_form(arguments.into(), item.into(), "command", |input| {
        message(input, "command", "Command")
    }))
}

/// Declares a query in one attribute: `#[cqrs::query(output = T, error =
/// E)]` is `#[derive(Query)]` with `#[query(...)]`.
#[proc_macro_attribute]
pub fn query(arguments: TokenStream, item: TokenStream) -> TokenStream {
    into_stream(attribute_form(arguments.into(), item.into(), "query", |input| {
        message(input, "query", "Query")
    }))
}

/// The attribute form of a derive whose helper is `helper`: the derive's
/// expansion with the attribute's arguments as its helper, and the item
/// without the helper attributes (no derive is left to read them).
fn attribute_form(
    arguments: TokenStream2,
    item: TokenStream2,
    helper: &str,
    derive: impl FnOnce(DeriveInput) -> Result<TokenStream2>,
) -> Result<TokenStream2> {
    let mut input: DeriveInput = syn::parse2(item)?;
    let helper_ident = Ident::new(helper, Span::call_site());
    if !arguments.is_empty() {
        input.attrs.push(syn::parse_quote!(#[#helper_ident(#arguments)]));
    }
    let generated = derive(input.clone())?;
    let is_helper = |attr: &syn::Attribute| attr.path().is_ident(helper);
    input.attrs.retain(|attr| !is_helper(attr));
    let strip_fields = |fields: &mut syn::Fields| {
        for field in fields.iter_mut() {
            field.attrs.retain(|attr| !is_helper(attr));
        }
    };
    match &mut input.data {
        Data::Struct(data) => strip_fields(&mut data.fields),
        Data::Enum(data) => {
            for variant in &mut data.variants {
                variant.attrs.retain(|attr| !is_helper(attr));
                strip_fields(&mut variant.fields);
            }
        }
        Data::Union(_) => {}
    }
    Ok(quote!(#input #generated))
}

// ---------------------------------------------------------------------------
// #[derive(Command)] / #[derive(Query)]
// ---------------------------------------------------------------------------

/// Declares a struct as a command: an intent to change state, sent with
/// `CommandGateway::send`.
///
/// `#[command(output = T, error = E)]` states what handling it returns;
/// `output` defaults to `()`, `error` is required. Implements `Message` and
/// the `Command` marker.
///
/// ```ignore
/// #[derive(Command)]
/// #[command(output = PostId, error = BlogError)]
/// struct CreatePost { title: String }
/// ```
#[proc_macro_derive(Command, attributes(command))]
pub fn derive_command(input: TokenStream) -> TokenStream {
    into_stream(message(parse_macro_input!(input), "command", "Command"))
}

/// Declares a struct as a query: a request for data that changes nothing,
/// sent with `QueryGateway::query`.
///
/// `#[query(output = T, error = E)]` states what it answers; `output`
/// defaults to `()`, `error` is required. Implements `Message` and the
/// `Query` marker, so a query can never be sent as a command (or the other
/// way around).
///
/// ```ignore
/// #[derive(Query)]
/// #[query(output = Option<PostSummary>, error = BlogError)]
/// struct FindPost { id: PostId }
/// ```
#[proc_macro_derive(Query, attributes(query))]
pub fn derive_query(input: TokenStream) -> TokenStream {
    into_stream(message(parse_macro_input!(input), "query", "Query"))
}

/// Shared expansion of `#[derive(Command)]` and `#[derive(Query)]`: reads
/// `#[<attribute>(output = T, error = E)]` and implements `Message` plus the
/// `marker` trait. `error` is required, `output` defaults to `()`.
fn message(input: DeriveInput, attribute: &str, marker: &str) -> Result<TokenStream2> {
    let mut output: Option<Type> = None;
    let mut error: Option<Type> = None;
    for attr in input.attrs.iter().filter(|attr| attr.path().is_ident(attribute)) {
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("output") {
                output = Some(meta.value()?.parse()?);
            } else if meta.path.is_ident("error") {
                error = Some(meta.value()?.parse()?);
            } else {
                return Err(meta.error("expected `output = Type` or `error = Type`"));
            }
            Ok(())
        })?;
    }
    let Some(error) = error else {
        return Err(Error::new_spanned(
            &input.ident,
            format!("missing `#[{attribute}(error = Type)]`: the error is part of the message's contract"),
        ));
    };
    let output = output.map_or_else(|| quote!(()), |ty| ty.to_token_stream());

    let krate = krate();
    let marker = Ident::new(marker, Span::call_site());
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    Ok(quote! {
        #[automatically_derived]
        impl #impl_generics #krate::Message for #name #ty_generics #where_clause {
            type Output = #output;
            type Error = #error;
        }

        #[automatically_derived]
        impl #impl_generics #krate::#marker for #name #ty_generics #where_clause {}
    })
}
