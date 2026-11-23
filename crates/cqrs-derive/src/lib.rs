//! Macros for the `cqrs` crate. Each one only writes what the user could
//! write by hand (trait impls, or the associated types of an impl); none of
//! them adds fields or rewrites a type.

mod aggregate_root;
mod handlers;
mod message_set;
mod projection;
mod query_updates;

use proc_macro::TokenStream;
use proc_macro_crate::{FoundCrate, crate_name};
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::{ToTokens, quote, quote_spanned};
use syn::{Data, DeriveInput, Error, Ident, LitInt, LitStr, Result, Type, parse_macro_input, spanned::Spanned};

/// Path to the `cqrs` crate, even when a consumer renamed the dependency.
fn krate() -> TokenStream2 {
    match crate_name("cqrs") {
        Ok(FoundCrate::Name(name)) => {
            let ident = Ident::new(&name, Span::call_site());
            quote!(::#ident)
        }
        // `cqrs` itself declares `extern crate self as cqrs`.
        Ok(FoundCrate::Itself) | Err(_) => quote!(::cqrs),
    }
}

/// An enum of commands becomes a command: `CreatePost(CreatePost), ...`.
///
/// `#[commands(error = E)]` is the error it fails with (each variant's
/// error converts `Into` it); its output is a generated `{Enum}Output`
/// with the same variants holding each command's output (`output = Name`
/// renames it, `derive(...)` derives traits on it). Each command converts
/// `From` into the enum. A gateway marked `#[gateway(commands = Enum)]`
/// takes the enum and hands each variant to its handler: what a transport
/// (a queue, a deserialized request) dispatches.
///
/// `port = Name` also generates a trait bounded by `CommandGateway` of every
/// command (and of the enum), implemented by every gateway that routes them:
/// what code depending on the commands names (`impl PostCommands`), whatever
/// gateway, interceptors or unit stand behind it.
///
/// ```ignore
/// #[derive(Commands)]
/// #[commands(error = BlogError, derive(Debug, PartialEq))]
/// enum PostCommand { CreatePost(CreatePost), PublishPost(PublishPost) }
/// ```
#[proc_macro_derive(Commands, attributes(commands))]
pub fn derive_commands(input: TokenStream) -> TokenStream {
    into_stream(message_set::message_set(
        parse_macro_input!(input),
        "commands",
        "Command",
    ))
}

/// The query counterpart of `#[derive(Commands)]`: `#[queries(error = E)]`,
/// routed by `#[gateway(queries = Enum)]`. `port = Name` generates the
/// query port, and `subscriptions = Name` a trait bounded by
/// `QuerySubscriptions` of every query.
#[proc_macro_derive(Queries, attributes(queries))]
pub fn derive_queries(input: TokenStream) -> TokenStream {
    into_stream(message_set::message_set(parse_macro_input!(input), "queries", "Query"))
}

/// A struct of `QueryUpdateEmitter<Q>` fields says where each query's
/// updates are emitted: it implements `UpdatesOf<Q>` per field, so
/// `SubscriptionQueries` can subscribe to any of them. Projections share it
/// (in an `Arc`) and emit through its fields.
///
/// ```ignore
/// #[derive(Default, QueryUpdates)]
/// pub struct PostUpdates {
///     pub find: QueryUpdateEmitter<FindPost>,
///     pub published: QueryUpdateEmitter<ListPublished>,
/// }
/// ```
#[proc_macro_derive(QueryUpdates)]
pub fn derive_query_updates(input: TokenStream) -> TokenStream {
    into_stream(query_updates::query_updates(parse_macro_input!(input)))
}

/// Implements `Aggregate` for a struct: `TYPE` from `#[aggregate(name =
/// "...")]` (default: the struct's name in snake case), the `Id` from the
/// field `id` (or the one marked `#[aggregate(id)]`), the initial state
/// with every other field at its `Default` (or `#[aggregate(initial =
/// path)]`, called with `&Id`), and `evolve` calling the struct's inherent
/// `apply(&mut self, &Event)` (or `#[aggregate(apply = method)]`).
///
/// `#[aggregate(tag = "course")]` builds the aggregate from every event
/// tagged `course=<id>`, whatever stream holds it (Axon 5's entity sourced
/// by tags, for dynamic consistency boundaries).
///
/// ```ignore
/// #[derive(AggregateRoot)]
/// #[aggregate(event = PostEvent)]
/// struct Post { id: PostId, title: String, published: bool }
///
/// impl Post {
///     fn apply(&mut self, event: &PostEvent) { /* ... */ }
/// }
/// ```
/// On `impl EventHandler for H`: fills in the trait's event type (from
/// `handle`'s `&EventEnvelope<Event>`) and unit of work (from its
/// `&mut ProcessingContext<'_, Tx>`; none named: any, so the handler runs
/// with every token store), and with `namespace = "..."` implements
/// `Projection`: the processor's name (Axon's `@Namespace`).
///
/// ```text
/// #[cqrs::projection(namespace = "post-summaries")]
/// impl EventHandler for PostSummaries {
///     async fn handle(&self, event: &EventEnvelope<PostEvent>, context: &mut ProcessingContext<'_>) -> Result<(), HandlerError> {
///         match &event.payload { /* ... */ }
///     }
/// }
/// ```
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

/// On `impl CommandHandler for H`: adds `type Command` and `type Event`,
/// read from `handle(&self, command: &Command, events: &mut Appender<Event>)`.
/// Types written by hand are kept.
///
/// ```ignore
/// #[command_handler]
/// impl<R: PostRepository, P: EventPublisher<Post>> CommandHandler for PublishPostHandler<R, P> {
///     async fn handle(&self, command: &PublishPost, events: &mut Appender<PostEvent>) -> Result<(), BlogError> { ... }
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

/// On `impl Handler for H`: adds `type Message`, read from
/// `handle(&self, query: Query)`. A type written by hand is kept.
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
/// An event also needs its tags ([`TagResolver`](derive@TagResolver)):
/// derive them, or implement the trait by hand.
#[proc_macro_derive(DomainEvent, attributes(event))]
pub fn derive_domain_event(input: TokenStream) -> TokenStream {
    into_stream(domain_event(parse_macro_input!(input)))
}

/// Implements `TagResolver` from the fields marked `#[tag]` (Axon's
/// `@EventTag`): each becomes a tag keyed by the field name, valued by its
/// `Display`; `#[tag = "key"]` names the key, and is required on tuple
/// fields. Without any, the event has no tags.
///
/// Leave it out to keep the event free of these attributes, and write
/// `impl TagResolver for MyEvent { fn tags(&self) -> Vec<Tag> { ... } }`.
#[proc_macro_derive(TagResolver, attributes(tag))]
pub fn derive_tag_resolver(input: TokenStream) -> TokenStream {
    into_stream(tag_resolver(parse_macro_input!(input)))
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

    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    Ok(quote! {
        #[automatically_derived]
        impl #impl_generics #krate::DomainEvent for #name #ty_generics #where_clause {
            fn event_type(&self) -> &'static str {
                #event_type
            }

            #version
        }
    })
}

/// Expansion of `#[derive(TagResolver)]`: a `fn tags` over the `#[tag]`
/// fields, or the trait's default (no tags) without any.
fn tag_resolver(input: DeriveInput) -> Result<TokenStream2> {
    let krate = krate();
    let name = &input.ident;
    if let Data::Union(_) = input.data {
        return Err(Error::new_spanned(name, "events cannot be unions"));
    }
    let tags = event_tags(&krate, &input)?;
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

// ---------------------------------------------------------------------------
// #[derive(Gateway)]
// ---------------------------------------------------------------------------

/// A struct of handlers becomes a gateway: for every field `f: H` it
/// implements `Dispatch<<H as Handler>::Message>`, forwarding to `self.f`.
///
/// ```ignore
/// #[derive(Gateway)]
/// struct PostCommands<R> {
///     create: CreatePostHandler<R>,
///     publish: PublishPostHandler<R>,
///     #[gateway(skip)]
///     config: Settings,
/// }
/// ```
///
/// No message is named: the route comes from each handler's
/// `type Message`. Two fields handling the same message are rejected by the
/// compiler (E0119, conflicting implementations); a field that is not a
/// handler fails on its `Handler` bound unless marked `#[gateway(skip)]`.
///
/// `#[gateway(commands = Enum)]` (or `queries = Enum`) also lets it take an
/// enum of `#[derive(Commands)]`, routing each variant to its handler.
#[proc_macro_derive(Gateway, attributes(gateway))]
pub fn derive_gateway(input: TokenStream) -> TokenStream {
    into_stream(gateway(parse_macro_input!(input)))
}

/// Expansion of `#[derive(Gateway)]`: one `Dispatch` impl per non-skipped
/// field, plus a compile-time `Handler` check for fields that do not depend
/// on the struct's generics.
fn gateway(input: DeriveInput) -> Result<TokenStream2> {
    let Data::Struct(data) = &input.data else {
        return Err(Error::new_spanned(&input.ident, "`Gateway` is derived on structs"));
    };

    let krate = krate();
    let name = &input.ident;
    let kind = message_set::gateway_kind(&input)?;
    let (impl_generics, ty_generics, _) = input.generics.split_for_impl();
    // A gateway of commands is generic over the publisher it is given.
    let mut with_publisher = input.generics.clone();
    with_publisher.params.push(syn::parse_quote!(__X));
    let (publisher_impl_generics, _, _) = with_publisher.split_for_impl();
    let mut impls = Vec::new();

    for (index, field) in data.fields.iter().enumerate() {
        if skipped(field)? {
            continue;
        }
        let access = field.ident.as_ref().map_or_else(
            || {
                let index = syn::Index::from(index);
                quote!(#index)
            },
            |ident| quote!(#ident),
        );
        let handler = &field.ty;
        let handler_trait = if kind.commands {
            quote!(CommandHandler)
        } else {
            quote!(Handler)
        };

        // A forgotten `#[gateway(skip)]` otherwise only shows up as a
        // confusing E0119. The check is only sound for types that do not use
        // the struct's generics: for those the derive cannot see the bounds
        // the handler impl needs.
        if !mentions_generics(handler, &input.generics) {
            impls.push(quote_spanned! {handler.span()=>
                #[allow(dead_code)]
                const _: () = {
                    fn is_handler<T: #krate::#handler_trait>() {}
                    fn check() {
                        is_handler::<#handler>();
                    }
                };
            });
        }

        if kind.commands {
            let message = quote!(<#handler as #krate::CommandHandler>::Command);
            let mut generics = with_publisher.clone();
            let where_clause = generics.make_where_clause();
            where_clause
                .predicates
                .push(syn::parse_quote!(#handler: #krate::CommandHandler));
            where_clause.predicates.push(syn::parse_quote!(
                __X: #krate::EventPublisher<<#handler as #krate::CommandHandler>::Event>
            ));
            where_clause.predicates.push(syn::parse_quote!(
                #name #ty_generics: ::core::marker::Send + ::core::marker::Sync
            ));
            impls.push(quote_spanned! {handler.span()=>
                #[automatically_derived]
                impl #publisher_impl_generics #krate::DispatchIn<#message, __X> for #name #ty_generics #where_clause {
                    fn dispatch_in(
                        &self,
                        message: #message,
                        metadata: #krate::Metadata,
                        publisher: &__X,
                    ) -> impl ::core::future::Future<Output = ::core::result::Result<
                        #krate::OutputOf<#message>,
                        #krate::ErrorOf<#message>,
                    >> + ::core::marker::Send {
                        #krate::dispatch_command(&self.#access, message, metadata, publisher)
                    }
                }
            });
            continue;
        }

        let message = quote!(<#handler as #krate::Handler>::Message);
        let mut generics = input.generics.clone();
        let where_clause = generics.make_where_clause();
        where_clause
            .predicates
            .push(syn::parse_quote!(#handler: #krate::Handler));
        // `Dispatch` is `Send + Sync`, and the `Handler` bound above only
        // covers this field. One bound on the whole struct, not one per
        // field: per-field bounds make N impls × N predicates, and the build
        // time grows quadratically with the number of handlers.
        where_clause.predicates.push(syn::parse_quote!(
            #name #ty_generics: ::core::marker::Send + ::core::marker::Sync
        ));
        impls.push(quote_spanned! {handler.span()=>
            #[automatically_derived]
            impl #impl_generics #krate::Dispatch<#message> for #name #ty_generics #where_clause {
                fn dispatch_with(
                    &self,
                    message: #message,
                    metadata: #krate::Metadata,
                ) -> impl ::core::future::Future<Output = ::core::result::Result<
                    #krate::OutputOf<#message>,
                    #krate::ErrorOf<#message>,
                >> + ::core::marker::Send {
                    <#handler as #krate::Handler>::handle_with(&self.#access, message, metadata)
                }
            }
        });
    }

    if impls.is_empty() {
        return Err(Error::new_spanned(name, "a gateway needs at least one handler field"));
    }

    // `#[gateway(commands = Enum)]` / `#[gateway(queries = Enum)]`: the
    // enum itself, each variant routed through the impls above.
    for set in &kind.sets {
        if kind.commands {
            let mut generics = with_publisher.clone();
            let where_clause = generics.make_where_clause();
            where_clause
                .predicates
                .push(syn::parse_quote!(#set: #krate::DispatchInVia<Self, __X>));
            where_clause
                .predicates
                .push(syn::parse_quote!(__X: ::core::marker::Send + ::core::marker::Sync));
            where_clause.predicates.push(syn::parse_quote!(
                #name #ty_generics: ::core::marker::Send + ::core::marker::Sync
            ));
            impls.push(quote_spanned! {set.span()=>
                #[automatically_derived]
                impl #publisher_impl_generics #krate::DispatchIn<#set, __X> for #name #ty_generics #where_clause {
                    fn dispatch_in(
                        &self,
                        message: #set,
                        metadata: #krate::Metadata,
                        publisher: &__X,
                    ) -> impl ::core::future::Future<Output = ::core::result::Result<
                        #krate::OutputOf<#set>,
                        #krate::ErrorOf<#set>,
                    >> + ::core::marker::Send {
                        <#set as #krate::DispatchInVia<Self, __X>>::dispatch_in_via(message, self, metadata, publisher)
                    }
                }
            });
            continue;
        }
        let mut generics = input.generics.clone();
        let where_clause = generics.make_where_clause();
        where_clause
            .predicates
            .push(syn::parse_quote!(#set: #krate::DispatchVia<Self>));
        where_clause.predicates.push(syn::parse_quote!(
            #name #ty_generics: ::core::marker::Send + ::core::marker::Sync
        ));
        impls.push(quote_spanned! {set.span()=>
            #[automatically_derived]
            impl #impl_generics #krate::Dispatch<#set> for #name #ty_generics #where_clause {
                fn dispatch_with(
                    &self,
                    message: #set,
                    metadata: #krate::Metadata,
                ) -> impl ::core::future::Future<Output = ::core::result::Result<
                    #krate::OutputOf<#set>,
                    #krate::ErrorOf<#set>,
                >> + ::core::marker::Send {
                    <#set as #krate::DispatchVia<Self>>::dispatch_via(message, self, metadata)
                }
            }
        });
    }
    Ok(quote!(#(#impls)*))
}

/// Whether the field is marked `#[gateway(skip)]` (not a handler, e.g.
/// configuration kept next to the handlers).
fn skipped(field: &syn::Field) -> Result<bool> {
    let mut skip = false;
    for attr in field.attrs.iter().filter(|attr| attr.path().is_ident("gateway")) {
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("skip") {
                skip = true;
                Ok(())
            } else {
                Err(meta.error("expected `skip`"))
            }
        })?;
    }
    Ok(skip)
}

/// Whether `ty` mentions any of the generic parameters (types, lifetimes or
/// consts) declared by `generics`.
fn mentions_generics(ty: &Type, generics: &syn::Generics) -> bool {
    let names: Vec<String> = generics
        .params
        .iter()
        .map(|param| match param {
            syn::GenericParam::Type(param) => param.ident.to_string(),
            syn::GenericParam::Lifetime(param) => param.lifetime.ident.to_string(),
            syn::GenericParam::Const(param) => param.ident.to_string(),
        })
        .collect();

    /// Searches the token tree, groups included, for any of `names`.
    fn walk(tokens: TokenStream2, names: &[String]) -> bool {
        tokens.into_iter().any(|token| match token {
            proc_macro2::TokenTree::Ident(ident) => names.contains(&ident.to_string()),
            proc_macro2::TokenTree::Group(group) => walk(group.stream(), names),
            _ => false,
        })
    }

    !names.is_empty() && walk(ty.to_token_stream(), &names)
}
