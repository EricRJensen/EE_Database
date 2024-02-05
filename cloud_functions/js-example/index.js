// Require client library and private key.
var ee = require('@google/earthengine');
var privateKey = require('./.private-key.json');
const functions = require('@google-cloud/functions-framework');

// Initialize client library and run analysis.
var runAnalysis = function() {
    
    ee.initialize(null, null, function() {
        var landsat = new ee.Image('LANDSAT/LC08/C01/T1_TOA/LC08_123032_20140515').select(['B4', 'B3', 'B2']);
        
        landsat.getMap({min: 0, max: 1000}, function(map) {
            console.log(map)
        });  
    })
}

// Authenticate using a service account.
ee.data.authenticateViaPrivateKey(privateKey, runAnalysis, function(e) {
    console.error('Authentication error: ' + e);
})

// Register an HTTP function with the Functions Framework that will be executed
// when you make an HTTP request to the deployed function's endpoint.
functions.http('runGET', (req, res) => {
});