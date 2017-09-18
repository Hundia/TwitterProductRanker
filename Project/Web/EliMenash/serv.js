var express = require("express");
var requireDir = require('require-dir');
var fs = require('fs');
var app = express();

var metaJson = requireDir('./DB');

var files = fs.readdirSync('./DB');

app.use(express.static('.'), function(req, res, next) {
    res.header("Access-Control-Allow-Origin", "*");
    res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
    next();
});

/* serves main page */
app.get("/", function(req, res) {
    res.sendFile('./Index.html', { root: '.' });
});

function getElementValByName(name) {
    var res = [];

    for(i = 0; i < files.length; i++) {

        filename = files[i].substring(0, files[0].indexOf(".json"));
        len = metaJson[filename].length;
        var index = -1;
        // find the element in the file
        for(j=0; j < len ; j++) {
            if(metaJson[filename][j][0] == name) {
                index = j;
                console.log('found at file: ' + filename + ' at index: ' + j + ' with val: ' + metaJson[filename][j][1]);
                break;
            }
        }

        if(index == -1) {
            console.info('didnt find element: ' + name + ' in file: ' + filename);
        }
        else {
            var tmp = {};
            // tmp['Month'] = month;
            // tmp['year'] = year;
            // tmp['monthPart'] = monthPart;
            tmp['val'] = metaJson[filename][index][1];
            res.push(tmp);
        }
    }

    return res;
}


app.get('/getNameData', function (req, res) {
    // console.log(req);

    // var year = req.query.year;
    // var month = req.query.month;
    // var monthPart = req.query.monthPart;
    var name = req.query.name;
    console.log('Got request for info on a name: ' + name);


    queryRes = getElementValByName(name);
    var queryResStr = JSON.stringify(queryRes);
    // console.log("Res: " + queryResStr);
    res.end(queryResStr);

})

// console.log(metaJson[files[0].substring(0, files[0].indexOf(".json"))][2][1]);


// var res = getElementValByName('http');
console.log(files.length);

var port = process.env.PORT || 5000;
app.listen(port, function() {
    console.log("Listening on " + port);
});