//! `#[derive(AggregateRoot)]`: an `Aggregate` impl from the struct, its
//! `#[aggregate(...)]` attribute and an inherent `apply` method.

use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{Data, DeriveInput, Error, Fields, Ident, LitStr, Path, Result, Type};

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
}

/// Expansion of `#[derive(AggregateRoot)]`.
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
                _ => return Err(meta.error("expected `name`, `event`, `tag`, `apply` or `initial`")),
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
        .or_else(|| fields.named.iter().find(|field| field.ident.as_ref().is_some_and(|ident| ident == "id")))
        .ok_or_else(|| Error::new_spanned(name, "no id: add a field `id`, or mark one `#[aggregate(id)]`"))?;
    let id = id_field.ident.as_ref().expect("named field");
    let id_type = &id_field.ty;

    let krate = krate();
    let type_name = attributes.name.unwrap_or_else(|| LitStr::new(&snake_case(&name.to_string()), name.span()));
    let apply = attributes.apply.unwrap_or_else(|| format_ident!("apply", span = name.span()));
    let initial = match attributes.initial {
        Some(path) => quote!(#path(id)),
        None => {
            let others = fields.named.iter().filter(|field| field.ident.as_ref() != Some(id)).map(|field| {
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
