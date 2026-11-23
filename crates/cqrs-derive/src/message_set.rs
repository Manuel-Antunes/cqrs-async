//! `#[derive(Commands)]` / `#[derive(Queries)]`: an enum whose variants are
//! messages becomes a message itself, answered with an enum of their
//! outputs, and routed variant by variant through a gateway.

use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{Data, DeriveInput, Error, Fields, Ident, Meta, Result, Type};

use crate::krate;

/// Expansion for an enum of commands (`marker = "Command"`) or queries.
pub fn message_set(input: DeriveInput, attribute: &str, marker: &str) -> Result<TokenStream2> {
    let name = &input.ident;
    if !input.generics.params.is_empty() {
        return Err(Error::new_spanned(&input.generics, "a message enum cannot be generic"));
    }
    let Data::Enum(data) = &input.data else {
        return Err(Error::new_spanned(name, format!("`{marker}s` is derived on an enum of {attribute}s")));
    };

    let mut error: Option<Type> = None;
    let mut output = format_ident!("{name}Output");
    let mut derives: Vec<syn::Path> = Vec::new();
    let mut port: Option<Ident> = None;
    let mut subscriptions: Option<Ident> = None;
    for attr in input.attrs.iter().filter(|attr| attr.path().is_ident(attribute)) {
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("error") {
                error = Some(meta.value()?.parse()?);
            } else if meta.path.is_ident("output") {
                output = meta.value()?.parse()?;
            } else if meta.path.is_ident("port") {
                port = Some(meta.value()?.parse()?);
            } else if meta.path.is_ident("subscriptions") && attribute == "queries" {
                subscriptions = Some(meta.value()?.parse()?);
            } else if meta.path.is_ident("derive") {
                meta.parse_nested_meta(|derive| {
                    derives.push(derive.path);
                    Ok(())
                })?;
            } else {
                return Err(meta.error(if attribute == "queries" {
                    "expected `error = Type`, `output = Name`, `port = Name`, `subscriptions = Name` or `derive(...)`"
                } else {
                    "expected `error = Type`, `output = Name`, `port = Name` or `derive(...)`"
                }));
            }
            Ok(())
        })?;
    }
    let Some(error) = error else {
        return Err(Error::new_spanned(
            name,
            format!("missing `#[{attribute}(error = Type)]`: what every variant's error converts into"),
        ));
    };

    let mut variants: Vec<(&Ident, &Type)> = Vec::new();
    for variant in &data.variants {
        match &variant.fields {
            Fields::Unnamed(fields) if fields.unnamed.len() == 1 => {
                variants.push((&variant.ident, &fields.unnamed[0].ty));
            }
            _ => {
                return Err(Error::new_spanned(
                    variant,
                    format!("each variant holds one {attribute}: `{}({})`", variant.ident, variant.ident),
                ));
            }
        }
    }
    if variants.is_empty() {
        return Err(Error::new_spanned(name, format!("a message enum needs at least one {attribute}")));
    }

    let krate = krate();
    let marker = Ident::new(marker, name.span());
    let vis = &input.vis;
    let idents: Vec<_> = variants.iter().map(|(ident, _)| *ident).collect();
    let types: Vec<_> = variants.iter().map(|(_, ty)| *ty).collect();
    let derive_attr = (!derives.is_empty()).then(|| quote!(#[derive(#(#derives),*)]));
    let output_doc = format!("What [`{name}`] answers: the output of the variant sent.");
    // Type-level checks (each variant is a command / query) without
    // depending on how the gateway is built.
    let checks = types.iter().map(|ty| {
        quote! { is_message::<#ty>(); }
    });

    // Ports naming what a gateway (or a `Cqrs` unit) exposes: one bound per
    // message, so code depending on them stays generic without listing
    // them.
    let gateway = if attribute == "queries" { quote!(QueryGateway) } else { quote!(CommandGateway) };
    let port = port.map(|port| {
        let doc = format!("Routes every message of [`{name}`], and the enum itself: implemented by any gateway that does.");
        quote! {
            #[doc = #doc]
            #vis trait #port: #(#krate::#gateway<#types> +)* #krate::#gateway<#name> {}
            #[automatically_derived]
            impl<T: ?::core::marker::Sized #(+ #krate::#gateway<#types>)* + #krate::#gateway<#name>> #port for T {}
        }
    });
    let subscriptions = subscriptions.map(|subscriptions| {
        let doc = format!("Subscribes to every query of [`{name}`]: implemented by any query gateway that does.");
        quote! {
            #[doc = #doc]
            #vis trait #subscriptions: #(#krate::QuerySubscriptions<#types>)+* {}
            #[automatically_derived]
            impl<T: ?::core::marker::Sized #(+ #krate::QuerySubscriptions<#types>)*> #subscriptions for T {}
        }
    });

    // Commands also route through a gateway of command handlers, with the
    // publisher their gateway was given.
    let dispatch_in_via = (attribute == "commands").then(|| {
        quote! {
            #[automatically_derived]
            impl<G, X> #krate::DispatchInVia<G, X> for #name
            where
                X: ::core::marker::Sync,
                #(G: #krate::DispatchIn<#types, X>, #krate::ErrorOf<#types>: ::core::convert::Into<#error>,)*
            {
                async fn dispatch_in_via(self, gateway: &G, metadata: #krate::Metadata, publisher: &X) -> ::core::result::Result<#output, #error> {
                    match self {
                        #(
                            Self::#idents(message) => <G as #krate::DispatchIn<#types, X>>::dispatch_in(gateway, message, metadata, publisher)
                                .await
                                .map(#output::#idents)
                                .map_err(::core::convert::Into::into),
                        )*
                    }
                }
            }
        }
    });

    Ok(quote! {
        #port
        #subscriptions

        #[doc = #output_doc]
        #derive_attr
        #vis enum #output {
            #(
                #[allow(missing_docs)]
                #idents(#krate::OutputOf<#types>),
            )*
        }

        #[automatically_derived]
        impl #krate::Message for #name {
            type Output = #output;
            type Error = #error;
        }

        #[automatically_derived]
        impl #krate::#marker for #name {}

        #(
            #[automatically_derived]
            impl ::core::convert::From<#types> for #name {
                fn from(message: #types) -> Self {
                    Self::#idents(message)
                }
            }
        )*

        #[automatically_derived]
        impl<G> #krate::DispatchVia<G> for #name
        where
            #(G: #krate::Dispatch<#types>, #krate::ErrorOf<#types>: ::core::convert::Into<#error>,)*
        {
            async fn dispatch_via(self, gateway: &G, metadata: #krate::Metadata) -> ::core::result::Result<#output, #error> {
                match self {
                    #(
                        Self::#idents(message) => <G as #krate::Dispatch<#types>>::dispatch_with(gateway, message, metadata)
                            .await
                            .map(#output::#idents)
                            .map_err(::core::convert::Into::into),
                    )*
                }
            }
        }

        #dispatch_in_via

        #[allow(dead_code)]
        const _: () = {
            fn is_message<T: #krate::#marker>() {}
            fn check() {
                #(#checks)*
            }
        };
    })
}

/// What `#[gateway(...)]` says: whether the gateway routes commands
/// (`commands`, `commands = Enum`: its fields are command handlers, which
/// commit through the publisher the gateway is given), and the enums it
/// also routes.
pub struct GatewayKind {
    /// `commands` or `commands = Enum`.
    pub commands: bool,
    /// The `commands = Enum` / `queries = Enum` types.
    pub sets: Vec<Type>,
}

/// Parses the struct's `#[gateway(...)]` attributes.
pub fn gateway_kind(input: &DeriveInput) -> Result<GatewayKind> {
    let mut kind = GatewayKind { commands: false, sets: Vec::new() };
    let mut queries = false;
    for attr in input.attrs.iter().filter(|attr| attr.path().is_ident("gateway")) {
        if let Meta::Path(_) = attr.meta {
            return Err(Error::new_spanned(attr, "expected `#[gateway(commands)]`, `#[gateway(commands = Enum)]` or `#[gateway(queries = Enum)]`"));
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("commands") {
                kind.commands = true;
                if meta.input.peek(syn::Token![=]) {
                    kind.sets.push(meta.value()?.parse()?);
                }
                Ok(())
            } else if meta.path.is_ident("queries") {
                queries = true;
                kind.sets.push(meta.value()?.parse()?);
                Ok(())
            } else {
                Err(meta.error("expected `commands`, `commands = Enum` or `queries = Enum`"))
            }
        })?;
    }
    if kind.commands && queries {
        return Err(Error::new_spanned(&input.ident, "a gateway routes commands or queries, not both: keep them in two gateways"));
    }
    Ok(kind)
}
