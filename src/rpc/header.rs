// Copyright 2025 NetApp Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

use super::Auth;

#[derive(Debug, PartialEq)]
pub(crate) struct Header {
    rpcvers: u32,
    prog: u32,
    vers: u32,
    proc: u32,
    cred: Auth,
    verf: Auth,
}

impl Header {
    pub(crate) fn new(
        rpcvers: u32,
        prog: u32,
        vers: u32,
        proc: u32,
        cred: &Auth,
        verf: &Auth,
    ) -> Header {
        Header {
            rpcvers,
            prog,
            vers,
            proc,
            cred: cred.clone(),
            verf: verf.clone(),
        }
    }

    pub(crate) fn encode(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.rpcvers.to_be_bytes());
        buf.extend_from_slice(&self.prog.to_be_bytes());
        buf.extend_from_slice(&self.vers.to_be_bytes());
        buf.extend_from_slice(&self.proc.to_be_bytes());
        self.cred.encode(buf);
        self.verf.encode(buf);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_new() {
        let unix = Auth::new_unix("localhost", 2, 3);
        assert_eq!(
            Header::new(9, 8, 7, 6, &Auth::new_null(), &Auth::new_null()),
            Header {
                rpcvers: 9,
                prog: 8,
                vers: 7,
                proc: 6,
                cred: Auth::new_null(),
                verf: Auth::new_null()
            }
        );
        assert_eq!(
            Header::new(9, 8, 7, 6, &unix, &Auth::new_null()),
            Header {
                rpcvers: 9,
                prog: 8,
                vers: 7,
                proc: 6,
                cred: unix.clone(),
                verf: Auth::new_null()
            }
        );
        assert_eq!(
            Header::new(9, 8, 7, 6, &Auth::new_null(), &unix),
            Header {
                rpcvers: 9,
                prog: 8,
                vers: 7,
                proc: 6,
                cred: Auth::new_null(),
                verf: unix.clone()
            }
        );
    }

    #[test]
    fn header_pack_null_cred() {
        let header = Header::new(1, 2, 3, 4, &Auth::new_null(), &Auth::new_null());
        let mut buf = Vec::<u8>::new();
        header.encode(&mut buf);
        let expected: Vec<u8> = vec![
            0, 0, 0, 1, // rpcvers
            0, 0, 0, 2, // prog
            0, 0, 0, 3, // vers
            0, 0, 0, 4, // proc
            0, 0, 0, 0, // cred auth flavor (AuthFlavor::Null)
            0, 0, 0, 0, // cred body length in bytes
            0, 0, 0, 0, // verf auth flavor (AuthFlavor::Null)
            0, 0, 0, 0, // verf body length in bytes
        ];
        assert_eq!(buf, expected);
        assert_eq!(buf.len(), expected.len());
    }

    #[test]
    fn header_pack_unix_cred() {
        let header = Header::new(
            4,
            3,
            2,
            1,
            &Auth::new_unix("machine", 350, 400),
            &Auth::new_null(),
        );
        let mut buf = Vec::<u8>::new();
        header.encode(&mut buf);
        let mut expected: Vec<u8> = vec![
            0, 0, 0, 4, // rpcvers
            0, 0, 0, 3, // prog
            0, 0, 0, 2, // vers
            0, 0, 0, 1, // proc
            0, 0, 0, 1, // cred auth flavor (AuthFlavor::Unix)
            0, 0, 0, 28, // cred body length in bytes
            0, 0, 0, 0, // stamp (random - hopefully never matches actual)
            0, 0, 0, 7,   // machinename length in bytes
            109, // 'm'
            97,  // 'a'
            99,  // 'c'
            104, // 'h'
            105, // 'i'
            110, // 'n'
            101, // 'e'
            0,   // padding
            0, 0, 1, 94, // uid (350)
            0, 0, 1, 144, // gid (400)
            0, 0, 0, 0, // gidlen
            0, 0, 0, 0, // verf auth flavor (AuthFlavor::Null)
            0, 0, 0, 0, // verf body length in bytes
        ];
        assert_ne!(buf, expected);
        expected.splice(24..28, buf[24..28].to_vec());
        assert_eq!(buf, expected);
    }
}
