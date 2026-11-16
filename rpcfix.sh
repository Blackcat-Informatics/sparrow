#!/bin/bash
set -e
f=rpc.rs
perl -0pi -e 's/                out.set_store_id\(self.store_id\(\)\);\n                out.set_payload_id\(&echoed\);/                out.set_store_id(&echoed.0);\n                out.set_payload_id(&echoed.1);/' $f
perl -0pi -e 's/        let echoed = payload_id.clone\(\);\n/        let echoed = (store_id, payload_id.clone());\n/' $f
perl -0pi -e 's/let input = \|\| -> Result<\(String, Option<String>, usize\), Error> \{\n            self.check_store\(\n                query\n                    .get_store_id\(\)\n                    .map_err\(invalid\)\?\n                    .to_str\(\)\n                    .map_err\(invalid\)\?,\n            \)\?;/let input = || -> Result<(String, String, Option<String>, usize), Error> {\n            let store_id = query\n                .get_store_id()\n                .map_err(invalid)?\n                .to_str()\n                .map_err(invalid)?\n                .to_owned();\n            self.check_store(&store_id)?;/' $f
perl -0pi -e 's/            Ok\(\(payload_id, cursor, query.get_page_size\(\) as usize\)\)/            Ok((store_id, payload_id, cursor, query.get_page_size() as usize))/' $f
perl -0pi -e 's/let \(payload_id, cursor, page_size\) = match input\(\)/let (store_id, payload_id, cursor, page_size) = match input()/' $f
