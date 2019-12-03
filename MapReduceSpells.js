const MongoClient = require('mongodb').MongoClient;
const uri = "mongodb://localhost:27017";
const client = new MongoClient(uri, { useNewUrlParser: true });
client.connect(err => {
    const collection = client.db("BD-Spell").collection("spells");
    // perform actions on the collection object
    function map1() {
        emit(this.titre, this.classe)
    };

    function red1(key, values) {
        return Array.sum(values);
    };

    let test = collection.mapReduce(
        map1,
        red1,
        {
            query: {resistance:"yes", component:" V", classe:{$regex:'sorcerer/wizard [0-4]'}}, 
            out: "map_reduce_Spell"
        }
    ).then(
        console.log("fini")
    )
    client.close()

});