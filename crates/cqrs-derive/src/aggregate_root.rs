//! `#[cqrs::aggregate(..)]`: an `Aggregate` impl from the struct, its
//! `#[aggregate(...)]` attribute and an inherent `apply` method, and the
//! `Snapshotting` impl saying how it is snapshotted.

use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{Data, DeriveInput, Error, Fields, Ident, LitInt, LitStr, Path, Result, Type, parse::Parser};

use crate::krate;

/// The arguments of `#[aggregate(...)]` on the struct.
#[derive(Default)]
struct Attributes {
    /// `name = "..."`: the stream prefix (`TYPE`).
    name: Option<LitStr>,
    /// `event = Type`: required.
    event: Option<Type>,
    /// `tag = "..."`: built from the events tagged `tag=id`.
    tag: Option<LitStr>,
    /// `apply = method`: what `evolve` calls; `apply` by default.
    apply: Option<Ident>,
    /// `initial = path`: builds the initial state from `&Id`.
    initial: Option<Path>,
    /// `snapshot(after_events = N, after_sourcing = "5s")`: when a load keeps
    /// a snapshot.
    snapshot: Option<Snapshot>,
}

/// The arguments of `snapshot(..)`: at least one threshold.
struct Snapshot {
    /// `after_events = N`: a load that replayed `N` events (at least 1).
    after_events: Option<u64>,
    /// `after_sourcing = "5s"`: a load that took this long, as the tokens of
    /// a `Duration`.
    after_sourcing: Option<TokenStream2>,
}

impl Snapshot {
    /// Reads `snapshot(..)`'s arguments.
    fn parse(meta: &syn::meta::ParseNestedMeta<'_>) -> Result<Self> {
        let mut after_events = None;
        let mut after_sourcing = None;
        let content;
        syn::parenthesized!(content in meta.input);
        let arguments: TokenStream2 = content.parse()?;
        // Checked here: an empty list is not a parse error of the nested
        // attribute, but a snapshot without a threshold.
        if arguments.is_empty() {
            return Err(Self::needs_a_threshold(meta));
        }
        syn::meta::parser(|inner| {
            if inner.path.is_ident("after_events") {
                if after_events.is_some() {
                    return Err(inner.error("`after_events` given twice"));
                }
                let literal: LitInt = inner.value()?.parse()?;
                let events: u64 = literal.base10_parse()?;
                if events == 0 {
                    return Err(Error::new_spanned(
                        literal,
                        "a snapshot after 0 events: `after_events` is at least 1",
                    ));
                }
                after_events = Some(events);
            } else if inner.path.is_ident("after_sourcing") {
                if after_sourcing.is_some() {
                    return Err(inner.error("`after_sourcing` given twice"));
                }
                after_sourcing = Some(duration(&inner.value()?.parse()?)?);
            } else {
                return Err(inner.error("expected `after_events` or `after_sourcing`"));
            }
            Ok(())
        })
        .parse2(arguments)?;
        if after_events.is_none() && after_sourcing.is_none() {
            return Err(Self::needs_a_threshold(meta));
        }
        Ok(Self {
            after_events,
            after_sourcing,
        })
    }

    /// `snapshot(..)` with nothing to say when to snapshot.
    fn needs_a_threshold(meta: &syn::meta::ParseNestedMeta<'_>) -> Error {
        meta.error("`snapshot(..)` needs a threshold: `after_events = 100`, `after_sourcing = \"5s\"`, or both")
    }
}

/// `"250ms"`, `"5s"` or `"2m"` as a `Duration`.
fn duration(literal: &LitStr) -> Result<TokenStream2> {
    let text = literal.value();
    let refuse = || {
        Error::new_spanned(
            literal,
            "`after_sourcing` is a whole number followed by `ms`, `s` or `m` (`\"250ms\"`, `\"5s\"`, `\"2m\"`)",
        )
    };
    let digits = text.chars().take_while(char::is_ascii_digit).count();
    let (number, unit) = text.split_at(digits);
    let number: u64 = number.parse().map_err(|_| refuse())?;
    match unit {
        "ms" => Ok(quote!(::core::time::Duration::from_millis(#number))),
        "s" => Ok(quote!(::core::time::Duration::from_secs(#number))),
        "m" => {
            let seconds = number.checked_mul(60).ok_or_else(refuse)?;
            Ok(quote!(::core::time::Duration::from_secs(#seconds)))
        }
        _ => Err(refuse()),
    }
}

/// Expansion of `#[cqrs::aggregate(..)]`.
pub fn aggregate_root(input: DeriveInput) -> Result<TokenStream2> {
    let mut attributes = Attributes::default();
    for attr in input.attrs.iter().filter(|attr| attr.path().is_ident("aggregate")) {
        attr.parse_nested_meta(|meta| {
            let key = meta.path.get_ident().map(ToString::to_string).unwrap_or_default();
            match key.as_str() {
                "name" => attributes.name = Some(meta.value()?.parse()?),
                "event" => attributes.event = Some(meta.value()?.parse()?),
                "tag" => attributes.tag = Some(meta.value()?.parse()?),
                "apply" => attributes.apply = Some(meta.value()?.parse()?),
                "initial" => attributes.initial = Some(meta.value()?.parse()?),
                "snapshot" => {
                    if attributes.snapshot.is_some() {
                        return Err(meta.error("`snapshot(..)` given twice"));
                    }
                    attributes.snapshot = Some(Snapshot::parse(&meta)?);
                }
                _ => {
                    return Err(meta.error("expected `name`, `event`, `tag`, `apply`, `initial` or `snapshot(..)`"));
                }
            }
            Ok(())
        })?;
    }
    let name = &input.ident;
    let Some(event) = attributes.event else {
        return Err(Error::new_spanned(name, "missing `#[aggregate(event = EventType)]`"));
    };
    let Data::Struct(data) = &input.data else {
        return Err(Error::new_spanned(name, "an aggregate root is a struct"));
    };
    let Fields::Named(fields) = &data.fields else {
        return Err(Error::new_spanned(name, "an aggregate root needs named fields"));
    };

    // The id: the field marked `#[aggregate(id)]`, else the one named `id`.
    let mut marked = None;
    for field in &fields.named {
        for attr in field.attrs.iter().filter(|attr| attr.path().is_ident("aggregate")) {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("id") {
                    marked = Some(field);
                    Ok(())
                } else {
                    Err(meta.error("expected `#[aggregate(id)]`"))
                }
            })?;
        }
    }
    let id_field = marked
        .or_else(|| {
            fields
                .named
                .iter()
                .find(|field| field.ident.as_ref().is_some_and(|ident| ident == "id"))
        })
        .ok_or_else(|| Error::new_spanned(name, "no id: add a field `id`, or mark one `#[aggregate(id)]`"))?;
    let id = id_field.ident.as_ref().expect("named field");
    let id_type = &id_field.ty;

    let krate = krate();
    let type_name = attributes
        .name
        .unwrap_or_else(|| LitStr::new(&snake_case(&name.to_string()), name.span()));
    let apply = attributes
        .apply
        .unwrap_or_else(|| format_ident!("apply", span = name.span()));
    let initial = match attributes.initial {
        Some(path) => quote!(#path(id)),
        None => {
            let others = fields
                .named
                .iter()
                .filter(|field| field.ident.as_ref() != Some(id))
                .map(|field| {
                    let ident = &field.ident;
                    quote!(#ident: ::core::default::Default::default())
                });
            quote!(Self { #id: ::core::clone::Clone::clone(id), #(#others,)* })
        }
    };
    let criteria = attributes.tag.map(|tag| {
        quote! {
            fn criteria(id: &#id_type) -> ::core::option::Option<#krate::EventCriteria> {
                ::core::option::Option::Some(#krate::EventCriteria::tagged([#krate::Tag::new(#tag, id)]))
            }
        }
    });

    let (mode, policy) = match attributes.snapshot {
        Some(Snapshot {
            after_events,
            after_sourcing,
        }) => {
            let policy = match (after_events, after_sourcing) {
                (Some(events), Some(time)) => {
                    quote!(#krate::SnapshotPolicy::after_events(#events).or_after_sourcing(#time))
                }
                (Some(events), None) => quote!(#krate::SnapshotPolicy::after_events(#events)),
                (None, Some(time)) => quote!(#krate::SnapshotPolicy::after_sourcing(#time)),
                (None, None) => unreachable!("`snapshot(..)` has a threshold"),
            };
            (quote!(#krate::Snapshots), quote!(::core::option::Option::Some(#policy)))
        }
        None => (quote!(#krate::NoSnapshots), quote!(::core::option::Option::None)),
    };

    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    Ok(quote! {
        #[automatically_derived]
        impl #impl_generics #krate::Aggregate for #name #ty_generics #where_clause {
            const TYPE: &'static str = #type_name;
            type Id = #id_type;
            type Event = #event;

            fn initial(id: &#id_type) -> Self {
                #initial
            }

            fn id(&self) -> &#id_type {
                &self.#id
            }

            fn evolve(&mut self, event: &#event) {
                // An inherent method: the derive cannot see it, so a
                // missing one is reported here.
                Self::#apply(self, event)
            }

            #criteria
        }

        #[automatically_derived]
        impl #impl_generics #krate::Snapshotting for #name #ty_generics #where_clause {
            type Mode = #mode;
            const POLICY: ::core::option::Option<#krate::SnapshotPolicy> = #policy;
        }
    })
}

/// `ShoppingCart` → `shopping_cart`.
fn snake_case(name: &str) -> String {
    let mut snake = String::new();
    for (index, character) in name.char_indices() {
        if character.is_uppercase() && index > 0 {
            snake.push('_');
        }
        snake.extend(character.to_lowercase());
    }
    snake
}
