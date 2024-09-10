use proc_macro::TokenStream;
use quote::quote;
use syn::{
    parse_macro_input, punctuated::Punctuated, Attribute, DeriveInput, Expr, ItemFn, Lit, LitStr, Meta, MetaNameValue, PatPath, Path, Token
};
#[proc_macro_attribute]
pub fn commandhandler(attr: TokenStream, item: TokenStream) -> TokenStream {
    // Parse the input function
    let input = parse_macro_input!(item as ItemFn);

    // Parse the attribute input as a comma-separated list of expressions
    let args = parse_macro_input!(attr with Punctuated::<Expr, Token![,]>::parse_terminated);

    // Check if the correct number of arguments is provided
    if args.len() != 3 {
        return syn::Error::new_spanned(
            args,
            "Expected exactly three arguments: `command` type, `error` type, and `path` string. Example usage: #[commandhandler(BankAccountCommand, BankAccountError, \"/path\")]",
        )
        .to_compile_error()
        .into();
    }

    // Extract the command type
    let command_type = match &args[0] {
        Expr::Path(PatPath { path, .. }) => path,
        _ => {
            return syn::Error::new_spanned(
                &args[0],
                "Expected the first argument to be a command type path.",
            )
            .to_compile_error()
            .into();
        }
    };

    // Extract the error type
    let error_type = match &args[1] {
        Expr::Path(PatPath { path, .. }) => path,
        _ => {
            return syn::Error::new_spanned(
                &args[1],
                "Expected the second argument to be an error type path.",
            )
            .to_compile_error()
            .into();
        }
    };

    // Extract the path string
    let path = match &args[2] {
        Expr::Lit(expr_lit) => {
            if let syn::Lit::Str(lit_str) = &expr_lit.lit {
                lit_str.value()
            } else {
                return syn::Error::new_spanned(
                    &expr_lit.lit,
                    "Expected the third argument to be a string literal for the path.",
                )
                .to_compile_error()
                .into();
            }
        }
        _ => {
            return syn::Error::new_spanned(
                &args[2],
                "Expected the third argument to be a string literal for the path.",
            )
            .to_compile_error()
            .into();
        }
    };

    // Generate the necessary utoipa and axum attributes with the derived types
    let generated = quote! {
        #[utoipa::path(
            post,
            path = #path,
            request_body = #command_type,
            responses(
                (status = 200, description = "Command dispatched successfully"),
                (status = 403, description = "Unauthorized"),
                (status = 422, description = "Domain validation error", body = #error_type)
            ),
            params(
                ("Authorization" = Option<String>, Header, description = "Bearer token"),
            ),
            security(
                ("bearerAuth" = [])
            )
        )]
        #input
    };

    generated.into()
}

// #[proc_macro_attribute]
// pub fn commandhandler(attr: TokenStream, item: TokenStream) -> TokenStream {
//     // Parse the input function
//     let input = parse_macro_input!(item as ItemFn);

//     // Parse the attribute input as two positional arguments: Path and LitStr
//     let args = parse_macro_input!(attr with Punctuated::<Expr, Token![,]>::parse_terminated);

//     // Expect exactly two arguments
//     if args.len() != 2 {
//         return syn::Error::new_spanned(
//             args,
//             "Expected exactly two arguments: `aggregate` type and `path` string",
//         )
//         .to_compile_error()
//         .into();
//     }
//     // Extract the aggregate type and path string
//     let aggregate_type = match &args[0] {
//         Expr::Path(PatPath { path, .. }) => path,
//         _ => {
//             return syn::Error::new_spanned(
//                 &args[0],
//                 "Expected the first argument to be an aggregate type path, such as a struct implementing a specific trait.",
//             )
//             .to_compile_error()
//             .into();
//         }
//     };

//     // // Extract the aggregate type and path string
//     // let aggregate_type = match &args[0] {
//     //     syn::Expr::Path(expr_path) => &expr_path.path,
//     //     _ => {
//     //         return syn::Error::new_spanned(
//     //             &args[0],
//     //             "Expected the first argument to be an aggregate type path",
//     //         )
//     //         .to_compile_error()
//     //         .into();
//     //     }
//     // };
//     // Extract the aggregate type and path string

//     let path = match &args[1] {
//         syn::Expr::Lit(expr_lit) => {
//             if let syn::Lit::Str(lit_str) = &expr_lit.lit {
//                 lit_str.value()
//             } else {
//                 return syn::Error::new_spanned(
//                     &expr_lit.lit,
//                     "Expected the second argument to be a string literal for the path",
//                 )
//                 .to_compile_error()
//                 .into();
//             }
//         }
//         _ => {
//             return syn::Error::new_spanned(
//                 &args[1],
//                 "Expected the second argument to be a string literal for the path",
//             )
//             .to_compile_error()
//             .into();
//         }
//     };

//     // Derive the command and error types based on the aggregate type
//     let command_type = quote! { #aggregate_type::Command };
//     let error_type = quote! { #aggregate_type::DomainError };

//     // Generate the necessary utoipa and axum attributes with the derived types
//     let generated = quote! {
//         #[utoipa::path(
//             post,
//             path = #path,
//             request_body = #command_type,
//             responses(
//                 (status = 200, description = "command dispatched successfully"),
//                 (status = 403, description = "Unauthorized"),
//                 (status = 422, description = "Domain validation error", body = #error_type)
//             ),
//             params(
//                 ("Authorization" = Option<String>, Header, description = "bearer token"),
//             ),
//             security(
//                 ("bearerAuth" = [])
//             )
//         )]
//         #input
//     };

//     generated.into()
// }

// // #[proc_macro_attribute]
// // pub fn commandhandler(attr: TokenStream, item: TokenStream) -> TokenStream {
// //     // Parse the attributes and the function

// //     // let args = parse_macro_input!(attr as AttributeArgs);

// //     let input = parse_macro_input!(item as ItemFn);
// //     let fn_name = &input.sig.ident;
// //     let args = parse_macro_input!(attr with Attribute::parse_outer);
// //     let mut aggregate_type: Option<Path> = None;
// //     let mut endpoint_path: Option<String> = None;
// //     // let ast: DeriveInput = syn::parse(attr).unwrap();

// //     // for myattr in ast.attrs {
// //     //     match &myattr {
// //     //         Meta::List(list) if list.path.is_ident("hello") => {
// //     //             list.parse_args_with(Punctuated::<LitStr, Token![,]>::parse_terminated)
// //     //                 .map_err(|_| {
// //     //                     // returning a specific syn::Error to teach the right usage of your macro
// //     //                     syn::Error::new(
// //     //                         list.span(),
// //     //                         // this indoc macro is just convenience and requires the indoc crate but can be done without it
// //     //                         indoc! {r#"
// //     //                             The `hello` attribute expects string literals to be comma separated

// //     //                             = help: use `#[hello("world1", "world2")]`
// //     //                         "#},
// //     //                     )
// //     //                 })?;
// //     //         }
// //     //         meta => {
// //     //             // returning a syn::Error would help with the compiler diagnostics and guide your macro users to get it right
// //     //             return Err(syn::Error::new(
// //     //                 meta.span(),
// //     //                 indoc! {r#"
// //     //                     The `hello` attribute is the only supported argument

// //     //                     = help: use `#[hello("world1")]`
// //     //                 "#},
// //     //             ));
// //     //         }
// //     //     }
// //     // }

// //     // Iterate over each attribute to find the aggregate type
// //     for attribute in args {
// //         attribute
// //             .parse_nested_meta(|meta| {
// //                 // Check if the meta is a name-value pair and matches "aggregate"
// //                 if meta.path.is_ident("aggregate") {
// //                     if let Lit::Str(lit_str) = meta.value()?.parse()? {
// //                         // Parse the literal string as a type path
// //                         aggregate_type = Some(lit_str.parse().expect("Expected a valid type path"));
// //                     }
// //                 } else if meta.path.is_ident("path") {
// //                     if let Lit::Str(lit_str) = meta.value()?.parse()? {
// //                         // Store the path string
// //                         endpoint_path = Some(lit_str.value());
// //                     }
// //                 }
// //                 Ok(())
// //             })
// //             .unwrap_or_else(|_| ());
// //     }

// //     // Check if the aggregate type was provided
// //     let aggregate = match aggregate_type {
// //         Some(ty) => ty,
// //         None => {
// //             return syn::Error::new_spanned(
// //                 input.sig.ident,
// //                 "Expected an `aggregate` parameter specifying the aggregate type.",
// //             )
// //             .to_compile_error()
// //             .into();
// //         }
// //     };
// //     // Extract the path string or return an error if not provided
// //     let path = match endpoint_path {
// //         Some(p) => p,
// //         None => {
// //             return syn::Error::new_spanned(
// //                 input.sig.ident,
// //                 "Expected a `path` parameter specifying the endpoint path.",
// //             )
// //             .to_compile_error()
// //             .into();
// //         }
// //     };

// //     // Initialize the variable to hold the aggregate type path
// //     // Derive the command and error types based on the aggregate type
// //     let command_type = quote! { #aggregate::Command };
// //     let error_type = quote! { #aggregate::DomainError };

// //     // Generate the utoipa attributes with the derived command and error types
// //     let generated = quote! {
// //         #[axum::debug_handler]
// //         #[utoipa::path(
// //             post,
// //             path = #path,
// //             request_body = #command_type,
// //             responses(
// //                 (status = 200, description = "Command dispatched successfully"),
// //                 (status = 403, description = "Unauthorized"),
// //                 (status = 422, description = "Domain validation error", body = #error_type)
// //             ),
// //             params(
// //                 ("Authorization" = Option<String>, Header, description = "bearer token"),
// //             ),
// //             security(
// //                 ("bearerAuth" = [])
// //             )
// //         )]
// //         #input
// //     };

// //     generated.into()
// // }
