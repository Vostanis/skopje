use proc_macro::TokenStream;
use quote::quote;
use syn::{
    Data, DeriveInput, Fields, FieldsNamed,
    parse::{Parse, ParseStream},
    parse_macro_input,
};

/// ```rust
/// #[derive(Debug, serde::Deserialize)]
/// #[skopje::extract(
///     method = HTTP_GET,
///     url = "https://api.binance.com/api/v1/ticker/allBookTickers",
/// )]
/// #[skopje::load(
///     method = PG_INSERT,
///     client = deadpool_postgres::Pool,
///     obj = self.0
/// )]
/// pub struct Symbols(pub Vec<Symbol>);
/// ```
///
/// Above is equivalent to:
///
/// ```rust
/// #[derive(Debug, serde::Deserialize)]
/// pub struct Symbols(pub Vec<Symbol>);
///
/// #[skopje::async_trait]
/// impl skopje::etl::Extract for Symbols {
///     type Client = skopje::HttpClient;
///     async fn extract(client: Self::Client) -> Result<Self> {
///         let url = "https://api.binance.com/api/v1/ticker/allBookTickers";
///         let data: Self = client.fetch(url).await?;
///         Ok(data)
///     }
/// }
///
/// #[skopje::async_trait]
/// impl skopje::etl::Load for Symbols {
///     type Client = skopje::PgPool;
///     async fn load(&self, client: Self::Client) -> Result<()> {
///         client
///             .insert(super::common_sql::INSERT_SYMBOL, self.0.iter())
///             .await?;
///         Ok(())
///     }
/// }
/// ```
#[proc_macro_attribute]
pub fn extract(item: TokenStream, attr: TokenStream) -> TokenStream {
    let args = parse_macro_input!(attr as ExtractArgs);
    quote! {}.into()
}

struct ExtractArgs<'a> {
    method: Option<&'a str>,
    url: Option<&'a str>,
    path: Option<&'a str>,
}

impl Parse for ExtractArgs<'_> {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut output = Self {
            method: None,
            url: None,
            path: None,
        };

        Ok(output)
    }
}

/// Provide a like-for-like implementation of ['crate::load::pg::SqlMap`].
/// Take the following:
///
/// ```rust
/// #[derive(SqlMap)]
/// struct MyStruct {
///     field0: String,
///     field1: i64,
/// }
/// ```
///
/// Above is equivalent to below:
///
/// ```rust
/// struct MyStruct {
///     field0: String,
///     field1: i64,
/// }
///
/// impl skopje::load::pg::SqlMap for &MyStruct {
///     fn sql_map(&self) -> std::vec::Vec<&(dyn skopje::ToSql + std::marker::Sync)> {
///         std::vec::Vec::from([
///             &self.field0,
///             &self.field1,
///         ])
///     }
/// }
/// ```
#[proc_macro_derive(SqlMap)]
pub fn derive_sql_map(item: TokenStream) -> TokenStream {
    let body = parse_macro_input!(item as DeriveInput);

    // Extract the struct name.
    let struct_name = &body.ident;

    // Extract field names.
    let fields = match &body.data {
        Data::Struct(data_struct) => match &data_struct.fields {
            Fields::Named(FieldsNamed { named, .. }) => named,
            _ => panic!("SqlMap can only be derived for structs with named fields"),
        },
        _ => panic!("SqlMap can only be derived for structs"),
    };

    // Create an array of references to each field.
    let field_refs = fields.iter().map(|field| {
        let field_name = &field.ident;
        quote! { &self.#field_name }
    });

    // Return the implementation.
    quote! {
        impl skopje::load::pg::SqlMap for &#struct_name {
            fn sql_map(&self) -> std::vec::Vec<&(dyn skopje::ToSql + std::marker::Sync)> {
                vec![#(#field_refs),*]
             }
        }
    }
    .into()
}
