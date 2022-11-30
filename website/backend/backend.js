const cors = require('cors')
const express = require('express')
const app = express()
const port = 3000
const mysql = require('mysql2')
router = express.Router();
const connection = mysql.createConnection({
  host: '35.204.42.188', //wieuncweuiocnweoucnbewiucw
  port: '3306',
  user: 'root',
  password: 'bigD',
  database: 'towers'
});
connection.connect();
async function queryDB(input) {
}
app.use(cors())
app.get('/:longitudeParameter/:lattitudeParameter', function(req, res) {
// console.log("Longitude: " + req.params.lonParam + "\nLatitude: " + req.params.latParam);
let sql = `SELECT * FROM towers WHERE st_distance_Sphere(point(lon, lat), point(${req.params.longitudeParameter}, ${req.params.lattitudeParameter})) <= towers.range*200`;
console.log(sql)
queryDB(sql, res);
});
function queryDB(sql, res) {
connection.query(sql, function(err, data, fields) {
    if (err) throw err;
    console.log("Erfolg")
    console.log(res)
    res.json({
    status: 200,
    data,
    message: "Cell_towers lists retrieved successfully"
    })
})
};
app.listen(port, () => {console.log(`listening on Port ${port}`)})