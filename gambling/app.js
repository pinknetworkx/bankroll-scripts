const config = require("./config");

const interval = require('interval-promise');

const { JsonRpc, Api } = require('eosjs');
const { JsSignatureProvider } = require('eosjs/dist/eosjs-jssig');
const fetch = require('node-fetch');

const signatureProvider = new JsSignatureProvider([config.finisherPrivateKey]);
const rpc = new JsonRpc(config.node, { fetch });
const api = new Api({ rpc, signatureProvider });

let pending_jobs = {};

async function update_cycles() {
    let rows = [];
    let resp = {"more": true};

    // fetch all open jobs
    while(resp.more) {
        resp = await rpc.get_table_rows({
            code: config.gamblingContract,
            json: true,
            scope: config.gamblingContract,
            table: "rolls",
            lower_bound: rows.length === 0 ? null : rows[rows.length - 1]["roll_id"],
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
        if(rows[i]["cycle_number"] === 0 || rows[i]["waiting_for_result"] === 1) {
            continue;
        }

        if(config.bannedCycleRolls.indexOf(rows[i]["roll_id"]) !== -1) {
            continue;
        }

        // if startroll fails, wait between the retries. Increase waiting time for each fail by 5 seconds
        if(pending_jobs[rows[i]["roll_id"]] && Date.now() < pending_jobs[rows[i]["roll_id"]]["last_try"] + 5000 * pending_jobs[rows[i]["roll_id"]]["failed"]) {
            continue;
        }

        let last_cycle = new Date(rows[i]["last_cycle"] + "+0000").getTime();
        let last_player_joined = new Date(rows[i]["last_player_joined"] + "+0000").getTime();

        // skip if is not ready
        if(last_cycle + rows[i]["cycle_time"] * 1000 >= Date.now()) {
            continue;
        }

        // skip if no player joined for one day
        if(last_player_joined + 24 * 3600 * 1000 <= Date.now()) {
            if(config.privilegedCycleRolls.indexOf(rows[i]["roll_id"]) === -1) {
                continue;
            }
        }

        console.log("finishing roll_id #" + rows[i]["roll_id"] + " / cycle_number " + rows[i]["cycle_number"]);

        promises.push(api.transact({
            actions: [{
                account: config.gamblingContract,
                name: 'startroll',
                authorization: [config.finisherAuthorization],
                data: {
                    roll_id: rows[i]["roll_id"]
                },
            }]
        }, {
            blocksBehind: 3,
            expireSeconds: 30,
        }).then(result => {
            console.log("roll_id #" + rows[i]["roll_id"] + " finished in transaction " + result["transaction_id"]);
        }).catch((e) => {
            if(pending_jobs[rows[i]["roll_id"]]) {
                pending_jobs[rows[i]["roll_id"]]["failed"] += 1;
                pending_jobs[rows[i]["roll_id"]]["last_try"] = Date.now();
            } else {
                pending_jobs[rows[i]["roll_id"]] = {
                    "failed": 1,
                    "last_try": Date.now()
                }
            }

            console.log(e)
        }));
    }

    await Promise.all(promises);
}

interval(async () => {
    try {
        await update_cycles();
    }
    catch (e) {
        console.log(e);
    }
}, 500);