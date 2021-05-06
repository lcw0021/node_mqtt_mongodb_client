// DB_NAME : smartcushion , COLLECTION_NAME : cushion

const mqtt = require('mqtt')
const mqtt_client = mqtt.connect('mqtt://localhost')

const MongoClient = require('mongodb').MongoClient;
const assert = require('assert');
const DB_URL = 'mongodb://localhost:27017';
const DB_NAME = "smartcushion";
const COLLECTION_NAME = "cushion";
const DB_CLIENT = new MongoClient(DB_URL);

mqtt_client.subscribe('/cushion/98');


async function INSERT_ONE(data){
    let mongo_client;
    try{
        mongo_client = await MongoClient.connect(DB_URL);
        console.log("DB connected..");
        const db = mongo_client.db(DB_NAME);

        let r = await db.collection(COLLECTION_NAME).insertOne(data);
        assert.equal(1,r.insertedCount);
        console.log("Inserted one..");

    }catch(err){
        console.log(err.stack);
    }
    mongo_client.close();
    console.log("DB closed..");
};

async function INSERT_MANY(data, size){
    let mongo_client;
    try{
        mongo_client = await MongoClient.connect(DB_URL);
        // console.log("DB connected..");
        const db = mongo_client.db(DB_NAME);

        let r = await db.collection(COLLECTION_NAME).insertMany(data);
        assert.equal(size,r.insertedCount);
        console.log(size + " Data Inserted");

    }catch(err){
        console.log(err.stack);
    }
    mongo_client.close();
    // console.log("DB closed..");
};





let CNT = 0;
const BUFFER_SIZE = 10;
let DATA_BUFFER = [];

mqtt_client.on('message', function(topic,message){
    // console.log("msg: "+message);
    let data = String(message)
    data = data.split(",");
    let data_time = Number(data[0]);
    let data_id = Number(data[1]);
    let data_type = String(data[2]);
    let data_value = new Array();
    for(let i=0; i<36; i++){
        data_value[i] = Number(data[i+3]);
    }
    let data_rssi = new Array();
    for(let i=0; i<5; i++){
        data_rssi[i] = Number(data[i+38]);
    }

    const insert_data = {
        time: data_time,
        id: data_id,
        type: data_type,
        value: data_value,
        rssi: data_rssi
    }

    if(CNT==BUFFER_SIZE){
        INSERT_MANY(DATA_BUFFER, BUFFER_SIZE);
        DATA_BUFFER = [];
        CNT = 0;
    }else{
        DATA_BUFFER.push(insert_data);
        CNT = CNT+1;
    }
    
    // INSERT_ONE(insert_data);
    // console.log(data);
});