// use arch_program::pubkey::Pubkey;
// use arch_sdk::{InputToSign, TransactionToSign};

// #[test]
// fn test_serialize_arch_program_deserialize_arch_sdk() {
//     // Create test data
//     let tx_bytes = vec![1, 2, 3, 4, 5, 6, 7, 8];
//     let signer = Pubkey::new_unique();
//     let program_id = Pubkey::new_unique();

//     // Test case 1: TransactionToSign with Sign input
//     {
//         let input_to_sign_program =
//             arch_program::input_to_sign::InputToSign::Sign { index: 0, signer };

//         let transaction_to_sign_program = arch_program::transaction_to_sign::TransactionToSign {
//             tx_bytes: &tx_bytes,
//             inputs_to_sign: &[input_to_sign_program],
//         };

//         // Serialize using arch_program
//         let serialized = transaction_to_sign_program.serialise();

//         // Deserialize using arch_sdk
//         let deserialized = TransactionToSign::from_slice(&serialized).unwrap();

//         // Verify the results
//         assert_eq!(deserialized.tx_bytes, tx_bytes);
//         assert_eq!(deserialized.inputs_to_sign.len(), 1);

//         match &deserialized.inputs_to_sign[0] {
//             InputToSign::Sign {
//                 index,
//                 signer: deserialized_signer,
//             } => {
//                 assert_eq!(index, &0);
//                 assert_eq!(deserialized_signer, &signer);
//             }
//             _ => panic!("Expected Sign variant"),
//         }
//     }

//     // Test case 2: TransactionToSign with SignWithSeeds input
//     {
//         let seed1 = b"seed1";
//         let seed2 = b"seed2";
//         let seeds: &[&[u8]] = &[seed1, seed2];

//         let input_to_sign_program = arch_program::input_to_sign::InputToSign::SignWithSeeds {
//             index: 1,
//             program_id,
//             signers_seeds: seeds,
//         };

//         let transaction_to_sign_program = arch_program::transaction_to_sign::TransactionToSign {
//             tx_bytes: &tx_bytes,
//             inputs_to_sign: &[input_to_sign_program],
//         };

//         // Serialize using arch_program
//         let serialized = transaction_to_sign_program.serialise();

//         // Deserialize using arch_sdk
//         let deserialized = TransactionToSign::from_slice(&serialized).unwrap();

//         // Verify the results
//         assert_eq!(deserialized.tx_bytes, tx_bytes);
//         assert_eq!(deserialized.inputs_to_sign.len(), 1);

//         match &deserialized.inputs_to_sign[0] {
//             InputToSign::SignWithSeeds {
//                 index,
//                 program_id: deserialized_program_id,
//                 signers_seeds,
//             } => {
//                 assert_eq!(index, &1);
//                 assert_eq!(deserialized_program_id, &program_id);
//                 assert_eq!(signers_seeds.len(), 2);
//                 assert_eq!(signers_seeds[0], seed1);
//                 assert_eq!(signers_seeds[1], seed2);
//             }
//             _ => panic!("Expected SignWithSeeds variant"),
//         }
//     }

//     // Test case 3: TransactionToSign with multiple inputs
//     {
//         let input1 = arch_program::input_to_sign::InputToSign::Sign { index: 0, signer };

//         let seed1 = b"test_seed";
//         let seeds: &[&[u8]] = &[seed1];
//         let input2 = arch_program::input_to_sign::InputToSign::SignWithSeeds {
//             index: 2,
//             program_id,
//             signers_seeds: seeds,
//         };

//         let transaction_to_sign_program = arch_program::transaction_to_sign::TransactionToSign {
//             tx_bytes: &tx_bytes,
//             inputs_to_sign: &[input1, input2],
//         };

//         // Serialize using arch_program
//         let serialized = transaction_to_sign_program.serialise();

//         // Deserialize using arch_sdk
//         let deserialized = TransactionToSign::from_slice(&serialized).unwrap();

//         // Verify the results
//         assert_eq!(deserialized.tx_bytes, tx_bytes);
//         assert_eq!(deserialized.inputs_to_sign.len(), 2);

//         // Check first input
//         match &deserialized.inputs_to_sign[0] {
//             InputToSign::Sign {
//                 index,
//                 signer: deserialized_signer,
//             } => {
//                 assert_eq!(index, &0);
//                 assert_eq!(deserialized_signer, &signer);
//             }
//             _ => panic!("Expected Sign variant for first input"),
//         }

//         // Check second input
//         match &deserialized.inputs_to_sign[1] {
//             InputToSign::SignWithSeeds {
//                 index,
//                 program_id: deserialized_program_id,
//                 signers_seeds,
//             } => {
//                 assert_eq!(index, &2);
//                 assert_eq!(deserialized_program_id, &program_id);
//                 assert_eq!(signers_seeds.len(), 1);
//                 assert_eq!(signers_seeds[0], seed1);
//             }
//             _ => panic!("Expected SignWithSeeds variant for second input"),
//         }
//     }

//     // Test case 4: Empty inputs
//     {
//         let transaction_to_sign_program = arch_program::transaction_to_sign::TransactionToSign {
//             tx_bytes: &tx_bytes,
//             inputs_to_sign: &[],
//         };

//         // Serialize using arch_program
//         let serialized = transaction_to_sign_program.serialise();

//         // Deserialize using arch_sdk
//         let deserialized = TransactionToSign::from_slice(&serialized).unwrap();

//         // Verify the results
//         assert_eq!(deserialized.tx_bytes, tx_bytes);
//         assert_eq!(deserialized.inputs_to_sign.len(), 0);
//     }
// }

// #[test]
// fn test_serialize_deserialize_with_large_data() {
//     // Create a larger transaction byte array
//     let tx_bytes: Vec<u8> = (0..1000).map(|i| (i % 256) as u8).collect();
//     let signer = Pubkey::new_unique();

//     let input_to_sign = arch_program::input_to_sign::InputToSign::Sign { index: 42, signer };

//     let transaction_to_sign_program = arch_program::transaction_to_sign::TransactionToSign {
//         tx_bytes: &tx_bytes,
//         inputs_to_sign: &[input_to_sign],
//     };

//     // Serialize using arch_program
//     let serialized = transaction_to_sign_program.serialise();

//     // Deserialize using arch_sdk
//     let deserialized = TransactionToSign::from_slice(&serialized).unwrap();

//     // Verify the results
//     assert_eq!(deserialized.tx_bytes, tx_bytes);
//     assert_eq!(deserialized.inputs_to_sign.len(), 1);

//     match &deserialized.inputs_to_sign[0] {
//         InputToSign::Sign {
//             index,
//             signer: deserialized_signer,
//         } => {
//             assert_eq!(index, &42);
//             assert_eq!(deserialized_signer, &signer);
//         }
//         _ => panic!("Expected Sign variant"),
//     }
// }

// #[test]
// fn test_sign_with_seeds_multiple_seeds() {
//     // Test SignWithSeeds with multiple seeds to ensure serialization is working correctly
//     let tx_bytes = vec![10, 20, 30, 40];
//     let program_id = Pubkey::new_unique();

//     let seed1 = b"longer_seed_name";
//     let seed2 = b"x";
//     let seed3 = b"medium";
//     let seeds: &[&[u8]] = &[seed1, seed2, seed3];

//     let input_to_sign_program = arch_program::input_to_sign::InputToSign::SignWithSeeds {
//         index: 5,
//         program_id,
//         signers_seeds: seeds,
//     };

//     let transaction_to_sign_program = arch_program::transaction_to_sign::TransactionToSign {
//         tx_bytes: &tx_bytes,
//         inputs_to_sign: &[input_to_sign_program],
//     };

//     // Serialize using arch_program
//     let serialized = transaction_to_sign_program.serialise();

//     // Deserialize using arch_sdk
//     let deserialized = TransactionToSign::from_slice(&serialized).unwrap();

//     // Verify the results
//     assert_eq!(deserialized.tx_bytes, tx_bytes);
//     assert_eq!(deserialized.inputs_to_sign.len(), 1);

//     match &deserialized.inputs_to_sign[0] {
//         InputToSign::SignWithSeeds {
//             index,
//             program_id: deserialized_program_id,
//             signers_seeds,
//         } => {
//             assert_eq!(index, &5);
//             assert_eq!(deserialized_program_id, &program_id);
//             assert_eq!(signers_seeds.len(), 3);
//             assert_eq!(signers_seeds[0], seed1);
//             assert_eq!(signers_seeds[1], seed2);
//             assert_eq!(signers_seeds[2], seed3);
//         }
//         _ => panic!("Expected SignWithSeeds variant"),
//     }
// }
