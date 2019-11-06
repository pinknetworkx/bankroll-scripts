const config = require("./config");

const ecc = require("eosjs-ecc");
const interval = require('interval-promise');

const { JsonRpc, Api } = require('eosjs');
const { JsSignatureProvider } = require('eosjs/dist/eosjs-jssig');
const fetch = require('node-fetch');

const signatureProvider = new JsSignatureProvider([config.oraclePrivateKey]);
const rpc = new JsonRpc(config.node, { fetch });
const api = new Api({ rpc, signatureProvider });

let pending_jobs = {};

async function update_oracle() {
    let rows = [];
    let resp = {"more": true};

    // fetch all open jobs
    while(resp.more) {
        resp = await rpc.get_table_rows({
            code: config.oracleAccount,
            json: true,
            scope: config.oracleAccount,
            table: "openjobs",
            lower_bound: rows.length === 0 ? null : rows[rows.length - 1]["id"],
            limit: 100
        });

        // first element is duplicate
        if(rows.length > 0) {
            resp.rows.shift();
        }

        // concat arrays
        rows = rows.concat(resp.rows)
    }

    if(rows.length === 0) {
        return;
    }

    let promises = [];
    for(let i = 0; i < rows.length; i++) {
        // if setrand fails, wait between the retries. Increase waiting time for each fail by 5 seconds
        if(pending_jobs[rows[i]["id"]] && Date.now() < pending_jobs[rows[i]["id"]]["last_try"] + 5000 * pending_jobs[rows[i]["id"]]["failed"]) {
            continue;
        }

        // sign hash
        let signing_hash = rows[i]["signing_hash"];
        let signature = ecc.signHash(signing_hash, config.signatureKey);

        console.log("successfully signed " + signing_hash + " with " + signature);

        promises.push(api.transact({
            actions: [{
                account: config.oracleAccount,
                name: 'setrand',
                authorization: [config.oracleAuthorization],
                data: {
                    job_id: rows[i]["id"],
                    sig: signature,
                },
            }]
        }, {
            blocksBehind: 3,
            expireSeconds: 30,
        }).then(result => {
            console.log("signature for " + signing_hash + " sent transaction id: " + result["transaction_id"]);
        }).catch((e) => {
            if(pending_jobs[rows[i]["id"]]) {
                pending_jobs[rows[i]["id"]]["failed"] += 1;
                pending_jobs[rows[i]["id"]]["last_try"] = Date.now();
            } else {
                pending_jobs[rows[i]["id"]] = {
                    "failed": 1,
                    "last_try": Date.now()
                }
            }

            console.log(e)
        }));
    }

    // wait for all jobs to be finished
    await Promise.all(promises);
}

interval(async () => {
    try {
        await update_oracle();
    }
    catch (e) {
        console.log(e);
    }
}, 500);