//! `#[event_handler]` on `impl EventHandler for H`: fills in the event type
//! and the unit of work from `handle`'s signature; with `namespace = ".."`,
//! makes it the handler of that namespace of `H`'s projection.
//! `#[projection(..)]` on the projection's struct: its name, events and
//! namespaces, and the route to their handlers.

use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{
    Error, FnArg, GenericArgument, GenericParam, ImplItem, ItemImpl, ItemStruct, LitStr, PathArguments, Result, Type,
    TypeReference, parse_quote, spanned::Spanned, visit_mut::VisitMut,
};

use crate::krate;

/// The arguments of `#[event_handler(...)]`.
#[derive(Default)]
pub struct HandlerArguments {
    /// `namespace = "..."`: the projection's namespace it handles.
    pub namespace: Option<LitStr>,
    /// `batch = N`: events per unit of work of the namespace's processor.
    pub batch: Option<syn::LitInt>,
}

impl syn::parse::Parse for HandlerArguments {
    fn parse(input: syn::parse::ParseStream) -> Result<Self> {
        let mut arguments = Self::default();
        while !input.is_empty() {
            let key: syn::Ident = input.parse()?;
            input.parse::<syn::Token![=]>()?;
            match key.to_string().as_str() {
                "namespace" => arguments.namespace = Some(input.parse()?),
                "batch" => {
                    let batch: syn::LitInt = input.parse()?;
                    if batch.base10_parse::<usize>()? == 0 {
                        return Err(Error::new(batch.span(), "a batch holds at least one event"));
                    }
                    arguments.batch = Some(batch);
                }
                _ => return Err(Error::new(key.span(), "expected `namespace = \"...\"` or `batch = N`")),
            }
            if !input.is_empty() {
                input.parse::<syn::Token![,]>()?;
            }
        }
        if let (Some(batch), None) = (&arguments.batch, &arguments.namespace) {
            return Err(Error::new(
                batch.span(),
                "`batch` is a namespace's: a handler run on its own gets it from its processor's options",
            ));
        }
        Ok(arguments)
    }
}

/// Expansion of `#[event_handler(...)]`.
pub fn event_handler(arguments: HandlerArguments, mut item: ItemImpl) -> Result<TokenStream2> {
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
    // No unit of work named: generic over any, so it runs with every
    // transaction manager; the contexts written `ProcessingContext<'_>` get
    // it too.
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

    let updated = item
        .items
        .iter()
        .find(|item| matches!(item, ImplItem::Fn(method) if method.sig.ident == "updated"));
    match (updated, &arguments.namespace) {
        (Some(updated), None) => {
            return Err(Error::new(
                updated.span(),
                "`updated` is a projection namespace's: it emits through the projection's `updates(..)` channels",
            ));
        }
        (Some(_), Some(_)) => item.items.insert(
            0,
            parse_quote!(
                const EMITS: bool = true;
            ),
        ),
        (None, _) => {}
    }

    if let Some(batch) = &arguments.batch {
        item.items.insert(
            0,
            parse_quote!(const BATCH_SIZE: ::core::option::Option<usize> = ::core::option::Option::Some(#batch);),
        );
    }
    let (_, trait_path, _) = item.trait_.as_mut().expect("checked above");
    // The path as written (`EventHandler`), still referred to below so its
    // import does not read as unused once the impl names another trait.
    let mut written = trait_path.clone();
    if let Some(last) = written.segments.last_mut() {
        last.arguments = PathArguments::None;
    }
    *trait_path = match &arguments.namespace {
        Some(namespace) => {
            parse_quote!(#krate::NamespaceHandler<{ #krate::namespace_id(#namespace) }, #event, #transaction>)
        }
        None => parse_quote!(#krate::EventHandler<#event, #transaction>),
    };
    Ok(quote! {
        #item
        const _: () = {
            #[allow(unused_imports)]
            use #written as _;
        };
    })
}

/// The arguments of `#[projection(...)]`.
pub struct ProjectionArguments {
    /// `name = "..."`: the prefix of its processors.
    name: LitStr,
    /// `events = Type`.
    events: Type,
    /// `namespaces("...", ...)`.
    namespaces: Vec<LitStr>,
    /// `updates(Query, ...)`: its subscription queries.
    updates: Vec<Type>,
    /// `subscriptions = Name`: the trait of whatever subscribes to them all.
    subscriptions: Option<syn::Ident>,
}

impl syn::parse::Parse for ProjectionArguments {
    fn parse(input: syn::parse::ParseStream) -> Result<Self> {
        let (mut name, mut events, mut namespaces) = (None, None, None);
        let (mut updates, mut subscriptions) = (Vec::new(), None);
        let span = input.span();
        while !input.is_empty() {
            let key: syn::Ident = input.parse()?;
            match key.to_string().as_str() {
                "name" => {
                    input.parse::<syn::Token![=]>()?;
                    name = Some(input.parse::<LitStr>()?);
                }
                "events" => {
                    input.parse::<syn::Token![=]>()?;
                    events = Some(input.parse::<Type>()?);
                }
                "namespaces" => {
                    let content;
                    syn::parenthesized!(content in input);
                    let list = content.parse_terminated(|input| input.parse::<LitStr>(), syn::Token![,])?;
                    namespaces = Some(list.into_iter().collect::<Vec<_>>());
                }
                "updates" => {
                    let content;
                    syn::parenthesized!(content in input);
                    let list = content.parse_terminated(<Type as syn::parse::Parse>::parse, syn::Token![,])?;
                    updates = list.into_iter().collect();
                }
                "subscriptions" => {
                    input.parse::<syn::Token![=]>()?;
                    subscriptions = Some(input.parse::<syn::Ident>()?);
                }
                _ => {
                    return Err(Error::new(
                        key.span(),
                        "expected `name = \"...\"`, `events = Type`, `namespaces(\"...\", ...)`, \
                         `updates(Query, ...)` or `subscriptions = TraitName`",
                    ));
                }
            }
            if !input.is_empty() {
                input.parse::<syn::Token![,]>()?;
            }
        }
        let name = name.ok_or_else(|| Error::new(span, "missing `name = \"...\"`"))?;
        let events = events.ok_or_else(|| Error::new(span, "missing `events = EventType`"))?;
        let namespaces = namespaces.ok_or_else(|| Error::new(span, "missing `namespaces(\"...\", ...)`"))?;
        if namespaces.is_empty() {
            return Err(Error::new(span, "a projection needs at least one namespace"));
        }
        for (index, namespace) in namespaces.iter().enumerate() {
            if namespaces[..index]
                .iter()
                .any(|other| other.value() == namespace.value())
            {
                return Err(Error::new(namespace.span(), "this namespace is listed twice"));
            }
        }
        for (index, query) in updates.iter().enumerate() {
            let text = quote!(#query).to_string();
            if updates[..index].iter().any(|other| quote!(#other).to_string() == text) {
                return Err(Error::new(query.span(), "this query is listed twice"));
            }
        }
        if let (Some(subscriptions), true) = (&subscriptions, updates.is_empty()) {
            return Err(Error::new(
                subscriptions.span(),
                "`subscriptions` names the trait of the queries in `updates(..)`: list them",
            ));
        }
        Ok(Self {
            name,
            events,
            namespaces,
            updates,
            subscriptions,
        })
    }
}

/// Expansion of `#[projection(...)]`: the struct, `Projection`, one
/// `HasNamespace` per namespace, and `Route` over every namespace's handler.
pub fn projection(arguments: ProjectionArguments, item: ItemStruct) -> Result<TokenStream2> {
    let krate = krate();
    let ProjectionArguments {
        name,
        events,
        namespaces,
        updates,
        subscriptions,
    } = arguments;
    let ident = &item.ident;
    let (impl_generics, ty_generics, where_clause) = item.generics.split_for_impl();
    let self_ty = quote!(#ident #ty_generics);
    let ids: Vec<_> = namespaces
        .iter()
        .map(|namespace| quote!({ #krate::namespace_id(#namespace) }))
        .collect();
    let indices: Vec<_> = (0..namespaces.len()).collect();
    // The struct's parameters have no bounds of their own: the projection is
    // shared by its processors, so it is `Send + Sync + 'static` itself.
    let mut projection_generics = item.generics.clone();
    projection_generics
        .make_where_clause()
        .predicates
        .push(parse_quote!(#ident #ty_generics: ::core::marker::Send + ::core::marker::Sync + 'static));
    let projection_where = projection_generics.where_clause.clone();
    let has: Vec<_> = namespaces
        .iter()
        .zip(&ids)
        .map(|(namespace, id)| {
            quote! {
                #[automatically_derived]
                impl #impl_generics #krate::HasNamespace<#id> for #self_ty #where_clause {
                    const NAME: &'static str = #namespace;
                }
            }
        })
        .collect();

    // `Route` for units of work `__CqrsTx`: the struct's generics, plus it.
    let mut route_generics = item.generics.clone();
    route_generics
        .params
        .push(GenericParam::Type(parse_quote!(__CqrsTx: ::core::marker::Send)));
    let route_where = route_generics.make_where_clause();
    route_where
        .predicates
        .push(parse_quote!(Self: #krate::Projection<Event = #events>));
    for id in &ids {
        route_where
            .predicates
            .push(parse_quote!(Self: #krate::NamespaceHandler<#id, #events, __CqrsTx>));
    }
    let (route_impl, _, route_where) = route_generics.split_for_impl();
    let handler = |id: &TokenStream2| quote!(<Self as #krate::NamespaceHandler<#id, #events, __CqrsTx>>);
    let handles = ids.iter().map(&handler);
    let resets = ids.iter().map(&handler);
    let replays = ids.iter().map(&handler);
    let batches = ids.iter().map(&handler);
    let emits = ids.iter().map(&handler);
    let updateds = ids.iter().map(&handler);
    let (i1, i3, i4, i5) = (&indices, &indices, &indices, &indices);
    let (i6, i7) = (&indices, &indices);
    let channels = channels(&item, &updates, subscriptions.as_ref());

    Ok(quote! {
        #item

        #[automatically_derived]
        impl #impl_generics #krate::Projection for #self_ty #projection_where {
            const NAME: &'static str = #name;
            type Event = #events;
            const NAMESPACES: &'static [&'static str] = &[#(#namespaces),*];
        }

        #(#has)*

        #channels

        #[automatically_derived]
        impl #route_impl #krate::Route<__CqrsTx> for #self_ty #route_where {
            async fn handle(
                &self,
                namespace: usize,
                event: &#krate::EventEnvelope<#events>,
                context: &mut #krate::ProcessingContext<'_, __CqrsTx>,
            ) -> ::core::result::Result<(), #krate::HandlerError> {
                match namespace {
                    #(#i1 => #handles::handle(self, event, context).await,)*
                    _ => ::core::result::Result::Ok(()),
                }
            }

            async fn reset(
                &self,
                namespace: usize,
                context: &mut #krate::ProcessingContext<'_, __CqrsTx>,
            ) -> ::core::result::Result<(), #krate::HandlerError> {
                match namespace {
                    #(#i3 => #resets::reset(self, context).await,)*
                    _ => ::core::result::Result::Ok(()),
                }
            }

            fn replays(namespace: usize) -> bool {
                match namespace {
                    #(#i4 => #replays::REPLAY,)*
                    _ => true,
                }
            }

            fn batch_size(namespace: usize) -> ::core::option::Option<usize> {
                match namespace {
                    #(#i5 => #batches::BATCH_SIZE,)*
                    _ => ::core::option::Option::None,
                }
            }

            fn emits(namespace: usize) -> bool {
                match namespace {
                    #(#i6 => #emits::EMITS,)*
                    _ => false,
                }
            }

            async fn updated(
                &self,
                namespace: usize,
                event: &#krate::EventEnvelope<#events>,
                updates: &<Self as #krate::HasUpdates>::Updates,
            ) {
                match namespace {
                    #(#i7 => #updateds::updated(self, event, updates).await,)*
                    _ => {}
                }
            }
        }
    })
}

/// The channels of the projection's subscription queries: a struct with a
/// `QueryUpdateChannel` per query in `updates(..)`, `HasUpdates` naming it
/// (`()` without), and the `subscriptions` trait if named.
fn channels(item: &ItemStruct, updates: &[Type], subscriptions: Option<&syn::Ident>) -> TokenStream2 {
    let krate = krate();
    let ident = &item.ident;
    let vis = &item.vis;
    let (impl_generics, ty_generics, where_clause) = item.generics.split_for_impl();
    if updates.is_empty() {
        return quote! {
            #[automatically_derived]
            impl #impl_generics #krate::HasUpdates for #ident #ty_generics #where_clause {
                type Updates = ();
            }
        };
    }
    let channels = syn::Ident::new(&format!("{ident}Updates"), ident.span());
    let fields: Vec<_> = (0..updates.len())
        .map(|index| syn::Ident::new(&format!("query_{index}"), ident.span()))
        .collect();
    let listed = updates
        .iter()
        .map(|query| format!("[`{}`]", quote!(#query).to_string().replace(' ', "")))
        .collect::<Vec<_>>()
        .join(", ");
    let doc = format!(
        "The subscription queries' channels of [`{ident}`] ({listed}): `QueryUpdateEmitter<{ident}>`, what its \
         namespaces' `updated` emits through."
    );
    let subscriptions = subscriptions.map(|subscriptions| {
        let doc =
            format!("Subscribes to every query [`{ident}`] updates ({listed}): implemented by any gateway that does.");
        quote! {
            #[doc = #doc]
            #vis trait #subscriptions: #(#krate::QuerySubscriptions<#updates>)+* {}
            #[automatically_derived]
            impl<T: ?::core::marker::Sized #(+ #krate::QuerySubscriptions<#updates>)*> #subscriptions for T {}
        }
    });
    quote! {
        #[doc = #doc]
        #[derive(Default)]
        #vis struct #channels {
            #(#fields: #krate::QueryUpdateChannel<#updates>,)*
        }

        impl #channels {
            /// Sends `update` to the subscriptions of query `Q` that
            /// `filter` accepts (`|query: &FindPost| query.id == id`);
            /// returns how many got it.
            #vis fn emit<Q: #krate::Query>(&self, filter: impl Fn(&Q) -> bool, update: #krate::OutputOf<Q>) -> usize
            where
                Self: #krate::UpdatesOf<Q>,
                #krate::OutputOf<Q>: ::core::clone::Clone,
            {
                <Self as #krate::UpdatesOf<Q>>::updates(self).emit(filter, update)
            }
        }

        #(
            #[automatically_derived]
            impl #krate::UpdatesOf<#updates> for #channels {
                fn updates(&self) -> &#krate::QueryUpdateChannel<#updates> {
                    &self.#fields
                }
            }
        )*

        #[automatically_derived]
        impl #impl_generics #krate::HasUpdates for #ident #ty_generics #where_clause {
            type Updates = #channels;
        }

        #subscriptions
    }
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
