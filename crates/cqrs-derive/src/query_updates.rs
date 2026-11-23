//! `#[derive(QueryUpdates)]`: a struct of `QueryUpdateEmitter<Q>` fields
//! becomes where each `Q`'s updates are emitted (`UpdatesOf<Q>`).

use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{Data, DeriveInput, Error, Fields, GenericArgument, PathArguments, Result, Type};

use crate::krate;

/// Expansion of `#[derive(QueryUpdates)]`.
pub fn query_updates(input: DeriveInput) -> Result<TokenStream2> {
    let name = &input.ident;
    let Data::Struct(data) = &input.data else {
        return Err(Error::new_spanned(
            name,
            "`QueryUpdates` is derived on a struct of emitters",
        ));
    };
    let Fields::Named(fields) = &data.fields else {
        return Err(Error::new_spanned(name, "`QueryUpdates` needs named fields"));
    };
    let krate = krate();
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    let mut impls = Vec::new();
    for field in &fields.named {
        // The query type is read from the field's type, not projected from
        // a trait: a projection would trip coherence across crates.
        let query = emitted_query(&field.ty)?;
        let ident = &field.ident;
        impls.push(quote! {
            #[automatically_derived]
            impl #impl_generics #krate::UpdatesOf<#query> for #name #ty_generics #where_clause {
                fn updates(&self) -> &#krate::QueryUpdateEmitter<#query> {
                    &self.#ident
                }
            }
        });
    }
    if impls.is_empty() {
        return Err(Error::new_spanned(
            name,
            "no emitter: add `QueryUpdateEmitter<Query>` fields",
        ));
    }
    Ok(quote!(#(#impls)*))
}

/// `Q` in a field of type `QueryUpdateEmitter<Q>`.
fn emitted_query(ty: &Type) -> Result<&Type> {
    let error = || Error::new_spanned(ty, "expected `QueryUpdateEmitter<Query>`");
    let Type::Path(path) = ty else {
        return Err(error());
    };
    let segment = path
        .path
        .segments
        .last()
        .filter(|segment| segment.ident == "QueryUpdateEmitter")
        .ok_or_else(error)?;
    let PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        return Err(error());
    };
    match arguments.args.first() {
        Some(GenericArgument::Type(query)) if arguments.args.len() == 1 => Ok(query),
        _ => Err(error()),
    }
}
