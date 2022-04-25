// Local test statement
// const process = { 
//     env: { 
//         SOURCE_API_URL: 'https://api.cmaww4dkcp-cantucome1-d1-public.model-t.cc.commerce.ondemand.com/occ/v2/basesites?fields=DEFAULT',
//         SOURCE_API_USER: 'mobile_android',
//         SOURCE_API_PWD: 'secret',

//         TARGET_DB_NAME: 'sapcloudintegration',
//         TARGET_DB_HOST: 'cantu-sql-server-dev.database.windows.net',
//         TARGET_DB_PORT: '1433',
//         TARGET_DB_USER: 'kymalogin',
//         TARGET_DB_PWD: 'YfjHTKbxz!zO',
//     }
// };

const process = { 
    env: { 
        SOURCE_API_URL: 'http://demo8849608.mockable.io/test',
        SOURCE_API_USER: 'mobile_android',
        SOURCE_API_PWD: 'secret',

        TARGET_DB_NAME: 'sapcloudintegration',
        TARGET_DB_HOST: 'cantu-sql-server-dev.database.windows.net',
        TARGET_DB_PORT: '1433',
        TARGET_DB_USER: 'kymalogin',
        TARGET_DB_PWD: 'YfjHTKbxz!zO',
    }
};



const request = require('request');
const { DataTypes, Sequelize } = require('sequelize');

const traceHeaders = ['x-request-id', 'x-b3-traceid', 'x-b3-spanid', 'x-b3-parentspanid', 'x-b3-sampled', 'x-b3-Flags', 'x-ot-span-context'];

var sourceApiUrl = `${process.env.SOURCE_API_URL}`;
var sourceApiUser = `${process.env.SOURCE_API_USER}`;
var sourceApiPwd = `${process.env.SOURCE_API_PWD}`;

var targetDbName = `${process.env.TARGET_DB_NAME}`;
var targetDbHost = `${process.env.TARGET_DB_HOST}`;
var targetDbPort = `${process.env.TARGET_DB_PORT}`;
var targetDbUser = `${process.env.TARGET_DB_USER}`;
var targetDbPwd = `${process.env.TARGET_DB_PWD}`;

var targetDbSchemaUpdate = `${process.env.TARGET_DB_SCHEMA_UPDATE}`;

const sequelize = new Sequelize(targetDbName, targetDbUser, targetDbPwd, {
    dialect: 'mssql',
    dialectOptions: {
        options: {
            useUTC: false,
            dateFirst: 1,
        }
    },
    host: targetDbHost,
    port: targetDbPort
});

const model = sequelize.define('Order', {
    PK: {
        type: DataTypes.STRING,
        primaryKey: true,
    },
    hjmpTS: DataTypes.STRING,
    createdTS: DataTypes.STRING,
    modifiedTS: DataTypes.STRING,
    TypePkString: DataTypes.STRING,
    OwnerPkString: DataTypes.STRING,
    sealed: DataTypes.STRING,
    p_calculated: DataTypes.STRING,
    p_code: DataTypes.STRING,
    p_currency: Sequelize.STRING,
    p_deliveryaddress: Sequelize.STRING,
    p_deliverycost: Sequelize.STRING,
    p_deliverymode: Sequelize.STRING,
    p_deliverystatus: Sequelize.STRING,
    p_description: Sequelize.STRING,
    p_expirationtime: Sequelize.STRING,
    p_globaldiscountvaluesinternal: Sequelize.STRING,
    p_name: Sequelize.STRING,
    p_net: Sequelize.STRING,
    p_paymentaddress: Sequelize.STRING,
    p_paymentcost: Sequelize.STRING,
    p_paymentinfo: Sequelize.STRING,
    p_paymentmode: Sequelize.STRING,
    p_paymentstatus: Sequelize.STRING,
    p_status: Sequelize.STRING,
    p_exportstatus: Sequelize.STRING,
    p_statusinfo: Sequelize.STRING,
    p_totalprice: Sequelize.STRING,
    p_totaldiscounts: Sequelize.STRING,
    p_totaltax: Sequelize.STRING,
    p_totaltaxvaluesinternal: Sequelize.STRING,
    p_user: Sequelize.STRING,
    p_subtotal: Sequelize.STRING,
    p_discountsincludedeliverycost: Sequelize.STRING,
    p_discountsincludepaymentcost: Sequelize.STRING,
    p_entrygroups: Sequelize.STRING,
    p_europe1pricefactory_udg: Sequelize.STRING,
    p_europe1pricefactory_upg: Sequelize.STRING,
    p_europe1pricefactory_utg: Sequelize.STRING,
    p_previousdeliverymode: Sequelize.STRING,
    p_site: Sequelize.STRING,
    p_store: Sequelize.STRING,
    p_guid: Sequelize.STRING,
    p_quotediscountvaluesinternal: Sequelize.STRING,
    p_consentreference: Sequelize.STRING,
    p_cartidreference: Sequelize.STRING,
    p_appliedcouponcodes: Sequelize.STRING,
    p_versionid: Sequelize.STRING,
    p_originalversion: Sequelize.STRING,
    p_fraudulent: Sequelize.STRING,
    p_potentiallyfraudulent: Sequelize.STRING,
    p_salesapplication: Sequelize.STRING,
    p_language: Sequelize.STRING,
    p_placedby: Sequelize.STRING,
    p_quotereference: Sequelize.STRING,
    aCLTS: Sequelize.STRING,
    propTS: Sequelize.STRING,
    p_fulfilmentstatus: Sequelize.STRING,
    p_notes: Sequelize.STRING,
    p_deliveryinfo: Sequelize.STRING,
    p_idanymarket: Sequelize.STRING,
    p_marketplaceid: Sequelize.STRING,
    p_marketplacenumber: Sequelize.STRING,
    p_marketplacedesc: Sequelize.STRING,
    p_statusanymarket: Sequelize.STRING,
    p_entregaesperada: Sequelize.STRING,
    p_marketplaceorderid: Sequelize.STRING,
    p_subtotalservice: Sequelize.STRING,
    p_subtotalwithoutdiscounts: Sequelize.STRING,
    p_migratedoldversion: Sequelize.STRING,
    p_migratedoldversionpk: Sequelize.STRING,
    p_customerip: Sequelize.STRING,
    p_originurl: Sequelize.STRING,
    p_clearsalestatuscode: Sequelize.STRING,
    p_clearsalesessionid: Sequelize.STRING,
    p_notifiedpickupstoredate: Sequelize.STRING,
    p_latlng: Sequelize.STRING,
    p_workshopinvoicedemailsent: Sequelize.STRING,
    p_workshopdeliveredemailsent: Sequelize.STRING,
    p_mediaorigininformation: Sequelize.STRING,
    p_sapplantcode: Sequelize.STRING,
    p_saprejectionreason: Sequelize.STRING,
    p_sapgoodsissuedate: Sequelize.STRING,
    p_invoicedbys4hana: Sequelize.STRING,
    p_manualtransfercompleted: Sequelize.STRING,
    p_ismultipledeliveryentries: Sequelize.STRING,
    p_anymarketfreight: Sequelize.STRING
}, {
    tableName: 'orders',
    name: 'Order'
})

if (true === targetDbSchemaUpdate){
    console.log(`********** Schema Update [TARGET_DB_SCHEMA_UPDATE] set as [TRUE]! Schema will be updated ...`);
    model.sync({force:true})
        .then(() => console.log('Schema successfully updated!'))
        .catch(e => console.log('Unable to update schema due error' + e));
} else {
    console.log(`********** Schema Update [TARGET_DB_SCHEMA_UPDATE] set as [FALSE]! Schema will NOT be updated.`);
}  


function dataSync(model, modelPk, traceCtxHeaders) {
    
    console.log("********** dataSync()");
    request.get({
        headers: traceCtxHeaders, url: sourceApiUrl, json: true,
        auth: {
            user: sourceApiUser,
            pass: sourceApiPwd,
            'sendImmediately': false
        }
    }, function (error, response, body) {
        
        if (error === null) {
            console.log(`********** Response.statusCode:\n${response.statusCode}`);
            if (response.statusCode == '200') {
                console.log('********** Response body:');
                console.log(body);
                console.log(`********** ${model.name} pk ${modelPk} found on SAP Commerce Cloud and must be upserted into Target DB`);
                upsert(model, modelPk, body);
            } else if (response.statusCode == '404') {
                console.log(`********** ${model.name} pk ${modelPk} not found on SAP Commerce Cloud and must be deleted from Target DB`);
                remove(model, modelPk);
            } else {
                console.log(`Call to AnalyticsSyncAPI.getOrderByPk webservice failed with status code ${response.statusCode}`);
                console.log('********** Response body:');
                console.log(response.body);
            }
        } else {
            console.log('********** Error:');
            console.log(error);
        }
    });
}

function upsert(model, modelPk, modelData){
    model.upsert(modelData)
        .then(([instance, created]) => {
            if (created){
                console.log(`********** ${model.name} pk ${modelPk} created into TARGET_DB successfully!`);
            } else {
                console.log(`********** ${model.name} pk ${modelPk} updated on TARGET_DB successfully!`);               
            }
        }, function(err){
            console.log(`********** ERROR trying to execute UPSERT for ${model.name} pk ${modelPk} into TARGET_DB.`);        
            console.log(err);
        });  
}

function remove(model, modelPk){
    const clause = {where: {PK: modelPk}}
    model.destroy(clause)
        .then(function(rowDeleted){
            if (rowDeleted === 1){
                console.log(`********** ${model.name} pk ${modelPk} deleted from TARGET_DB successfully!`);
            }
        }, function(err){
            console.log(`********** ERROR trying to execute REMOVE for ${model.name} pk ${pk} from TARGET_DB.`);        
            console.log(err);
        });
}

function extractTraceHeaders(headers) {
    console.log("********** extractTraceHeaders()");
    console.log(headers);
    var map = {};
    for (var i in traceHeaders) {
        h = traceHeaders[i];
        headerVal = headers[h];
        console.log('header' + h + "-" + headerVal);
        if (headerVal !== undefined) {
            console.log('if not undefined header' + h + "-" + headerVal);
            map[h] = headerVal;
        }
    }
    return map;
}

module.exports = { 

    main: function (event, context) {

        console.log('********** Event Data:');
        console.log(event.data);

        var modelPk = event.data.pk;

        var traceCtxHeaders = extractTraceHeaders(event.extensions.request.headers);

        dataSync(model, modelPk, traceCtxHeaders);
    }
};


//Local test statement
const event = {
    data: {
        pk: '1234'}, 
        extensions: { 
            request: {
                headers: {} 
            }
        }
    
    }
const context = {};

module.exports.main(event, context);