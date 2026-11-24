//! `#[command_gateway(...)]` / `#[query_gateway(...)]`: a struct that
//! handles the messages it lists, one handler impl each, becomes their
//! gateway.

use proc_macro2::TokenStream as TokenStream2;
use quote::{quote, quote_spanned};
use syn::{Error, Fields, Ident, ItemStruct, Result, Type, parse::ParseStream, spanned::Spanned};

use crate::krate;

/// Whether a gateway routes commands or queries.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// `#[command_gateway]`.
    Commands,
    /// `#[query_gateway]`.
    Queries,
}

/// The arguments of `#[command_gateway(...)]` / `#[query_gateway(...)]`.
pub struct GatewayArguments {
    /// `handles(A, B, ...)`: the messages routed.
    handles: Vec<Type>,
    /// `commands = Name` / `queries = Name`: the trait of every gateway
    /// routing them.
    port: Option<Ident>,
}

impl GatewayArguments {
    /// Parses the arguments of a gateway of `kind`.
    pub fn parse(kind: Kind, input: ParseStream) -> Result<Self> {
        let port_key = match kind {
            Kind::Commands => "commands",
            Kind::Queries => "queries",
        };
        let expected = match kind {
            Kind::Commands => "expected `handles(Command, ...)` or `commands = TraitName`",
            Kind::Queries => {
                "expected `handles(Query, ...)` or `queries = TraitName` (subscriptions are the projection's: \
                              `#[cqrs::projection(.., updates(..), subscriptions = TraitName)]`)"
            }
        };
        let span = input.span();
        let (mut handles, mut port) = (None, None);
        while !input.is_empty() {
            let key: Ident = input.parse()?;
            match key.to_string().as_str() {
                "handles" => {
                    let content;
                    syn::parenthesized!(content in input);
                    let list = content.parse_terminated(<Type as syn::parse::Parse>::parse, syn::Token![,])?;
                    handles = Some(list.into_iter().collect::<Vec<_>>());
                }
                key_name if key_name == port_key => {
                    input.parse::<syn::Token![=]>()?;
                    port = Some(input.parse()?);
                }
                _ => return Err(Error::new(key.span(), expected)),
            }
            if !input.is_empty() {
                input.parse::<syn::Token![,]>()?;
            }
        }
        let what = match kind {
            Kind::Commands => "command",
            Kind::Queries => "query",
        };
        let handles = handles.ok_or_else(|| {
            Error::new(
                span,
                format!(
                    "missing `handles({}, ...)`: the {what}s this gateway routes",
                    capitalize(what)
                ),
            )
        })?;
        if handles.is_empty() {
            return Err(Error::new(span, format!("a gateway handles at least one {what}")));
        }
        for (index, message) in handles.iter().enumerate() {
            let text = quote!(#message).to_string();
            if handles[..index].iter().any(|other| quote!(#other).to_string() == text) {
                return Err(Error::new(message.span(), "this message is listed twice"));
            }
        }
        Ok(Self { handles, port })
    }
}

/// `word` with its first letter in upper case.
fn capitalize(word: &str) -> String {
    let mut chars = word.chars();
    chars
        .next()
        .map(|first| first.to_uppercase().chain(chars).collect())
        .unwrap_or_default()
}

/// Expansion: the struct (without its `#[gateway(...)]` field attributes),
/// a check that it handles every listed message, a `Dispatch` per message,
/// and the traits named.
pub fn gateway(kind: Kind, arguments: GatewayArguments, mut item: ItemStruct) -> Result<TokenStream2> {
    let krate = krate();
    let unit = take_unit(&mut item)?;
    let name = &item.ident;
    let vis = &item.vis;
    let (impl_generics, ty_generics, where_clause) = item.generics.split_for_impl();
    let GatewayArguments { handles, port } = arguments;

    let unit = match (kind, unit) {
        (Kind::Commands, Some(unit)) => Some(unit),
        (Kind::Commands, None) => {
            return Err(Error::new_spanned(
                name,
                "a command gateway needs the unit of work its commands run in: a field marked `#[gateway(unit)]` \
                 (e.g. the unit's `Events`, or an `InlinePublisher`)",
            ));
        }
        (Kind::Queries, Some((_, ty))) => {
            return Err(Error::new_spanned(
                ty,
                "queries run in no unit of work: drop `#[gateway(unit)]`",
            ));
        }
        (Kind::Queries, None) => None,
    };

    // Every listed message has its handler impl, whatever the impl's bounds:
    // checked here, where the gateway is declared.
    let check = quote! {
        #[allow(dead_code)]
        const _: () = {
            trait __CqrsHandlesAll: #(#krate::Handles<#handles> +)* {}
            impl #impl_generics __CqrsHandlesAll for #name #ty_generics #where_clause {}
        };
    };

    let dispatches = handles.iter().map(|message| {
        let mut generics = item.generics.clone();
        let predicates = &mut generics.make_where_clause().predicates;
        let body = match &unit {
            Some((access, unit_ty)) => {
                let handler = quote!(<Self as #krate::CommandHandler<#message>>);
                predicates.push(syn::parse_quote!(Self: #krate::CommandHandler<#message>));
                predicates.push(syn::parse_quote!(
                    #unit_ty: #krate::UnitOfWork<#handler::Event, #handler::Unit>
                ));
                predicates.push(syn::parse_quote!(
                    #krate::ErrorOf<#message>: ::core::convert::From<#krate::StoreError>
                ));
                quote!(#krate::dispatch_command(self, message, metadata, &self.#access))
            }
            None => {
                predicates.push(syn::parse_quote!(Self: #krate::QueryHandler<#message>));
                quote!(<Self as #krate::QueryHandler<#message>>::handle_with(self, message, metadata))
            }
        };
        let (impl_generics, _, where_clause) = generics.split_for_impl();
        quote_spanned! {message.span()=>
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
                    #body
                }
            }
        }
    });

    let (gateway_trait, what) = match kind {
        Kind::Commands => (quote!(CommandGateway), "command"),
        Kind::Queries => (quote!(QueryGateway), "query"),
    };
    let listed = handles
        .iter()
        .map(|message| format!("[`{}`]", quote!(#message).to_string().replace(' ', "")))
        .collect::<Vec<_>>()
        .join(", ");
    let port = port.map(|port| {
        let doc = format!(
            "Sends every {what} [`{name}`] handles ({listed}): implemented by any gateway that does, so code \
             depending on them names this trait, whatever gateway, interceptors or unit stand behind it."
        );
        quote! {
            #[doc = #doc]
            #vis trait #port: #(#krate::#gateway_trait<#handles>)+* {}
            #[automatically_derived]
            impl<T: ?::core::marker::Sized #(+ #krate::#gateway_trait<#handles>)*> #port for T {}
        }
    });
    Ok(quote! {
        #item
        #check
        #(#dispatches)*
        #port
    })
}

/// Removes the `#[gateway(...)]` attributes of the struct's fields; returns
/// the field marked `#[gateway(unit)]` (how to reach it, and its type).
fn take_unit(item: &mut ItemStruct) -> Result<Option<(TokenStream2, Type)>> {
    let mut unit = None;
    let fields = match &mut item.fields {
        Fields::Named(fields) => fields.named.iter_mut().collect::<Vec<_>>(),
        Fields::Unnamed(fields) => fields.unnamed.iter_mut().collect(),
        Fields::Unit => Vec::new(),
    };
    for (index, field) in fields.into_iter().enumerate() {
        let mut is_unit = false;
        for attr in field.attrs.iter().filter(|attr| attr.path().is_ident("gateway")) {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("unit") {
                    is_unit = true;
                    Ok(())
                } else {
                    Err(meta.error("expected `#[gateway(unit)]`"))
                }
            })?;
        }
        field.attrs.retain(|attr| !attr.path().is_ident("gateway"));
        if !is_unit {
            continue;
        }
        if unit.is_some() {
            return Err(Error::new_spanned(&field.ty, "a gateway has one `#[gateway(unit)]`"));
        }
        let access = field.ident.as_ref().map_or_else(
            || {
                let index = syn::Index::from(index);
                quote!(#index)
            },
            |ident| quote!(#ident),
        );
        unit = Some((access, field.ty.clone()));
    }
    Ok(unit)
}
