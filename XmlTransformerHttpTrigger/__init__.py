import azure.functions as func
import pandas as pd
import datetime
import logging
import xmltodict
from azure.storage.blob import ContainerClient
import bmtx.KitchenSinkXml as ksx

dateStr = str(datetime.datetime.now().strftime("%Y-%m-%d"))
conn_str = "DefaultEndpointsProtocol=https;AccountName=adldataopscentralus;AccountKey=DYJ1kZ3IrsLbBMlNDmgIWHkSoK36GHjUumhnb2w4H0kUbVpzs4cCfrQ6Z9gf2kINHz9AtdzVkswKBosdryIGpg==;EndpointSuffix=core.windows.net"
container_str = "kitchensink"
container_client = ContainerClient.from_connection_string(
    conn_str=conn_str, 
    container_name=container_str
)
      
#test
def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('name')
    tag = req.params.get('tag')
    lob = req.params.get('lob')
    blobName = req.params.get('blobName')

    if not blobName:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            blobName = req_body.get('blobName')

    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if not tag:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            tag = req_body.get('tag')         

    if not lob:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            lob = req_body.get('lob')                  
    
    
    transformer = ksx.KitchenSinkXmlTransformer()
    logging.info('Transformer: ' + transformer.getName());

    if (name == 'KitchenSink'):
        if (lob == 'TMM'):
            logging.info('processKitchenSink...')
            transformer.processKitchenSink(lob,tag,blobName,container_client)
        elif (lob == 'VIBE'):
            logging.info('processKitchenSink - VIBE')  
            processKitchenSink(lob,tag,blobName)          
    
    return func.HttpResponse("This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",status_code=200)    

def processAlerts(d,dfAlerts,tnxId):
    logging.info('processAlerts...' + str(tnxId))

    try:
        d = d['Alerts']
        i = 0

        for a in d['Alerts']:
            dfValues = pd.DataFrame.from_dict(d['Alerts'][i].items())
            dfValues = dfValues.T
            dfValues.columns = [s.replace("@", "") for s in list(d['Alerts'][i].keys())]

            if (i == 0):
                df = dfValues.iloc[[1]]
            else:
                tempDf = dfValues.iloc[[1]]
                df = df.append(tempDf)

            i += 1    

        df.insert(0,'AcctTxnLogId',tnxId)
        dfAlerts = dfAlerts.append(df)
    except Exception as e:
        logging.info ("No Alerts")       
        
    return dfAlerts
    
def processRisks(d,dfRisks,tnxId):
    logging.info('processRisks...' + str(tnxId))

    if d['Risks']:
        try:
            i = 0

            for a in d['Risks']['Risks']:
                dfValues = pd.DataFrame.from_dict(d['Risks']['Risks'][i].items())    
                dfValues = dfValues.T
                dfValues.columns = [s.replace("@", "") for s in list(d['Risks']['Risks'][i].keys())]

                if (i == 0):
                    df = dfValues.iloc[[1]]
                else:
                    tempDf = dfValues.iloc[[1]]
                    df = df.append(tempDf)

                i += 1
            
            df.insert(0,'AcctTxnLogId',tnxId)
            dfRisks = dfRisks.append(df)     

        except KeyError:
            key = '@DepositRiskFactorId'

            if key in d['Risks']['Risks']:
                dfValues = pd.DataFrame.from_dict(d['Risks']['Risks'].items())
                dfValues = dfValues.T   
                dfValues.columns = [s.replace("@", "") for s in list(d['Risks']['Risks'].keys())]
                df = dfValues.iloc[[1]]   

                df.insert(0,'AcctTxnLogId',tnxId)
                dfRisks = dfRisks.append(df) 
            else:
                logging.info('No key...')

    return dfRisks                   

def processReviews(d,dfReviews,tnxId):
    logging.info('processReviews...' + str(tnxId))

    d = d['Reviews']
    key = '@DepositItemReviewLogId'
    
    if key in d['Review']:
        try:
            i = 0

            for a in d:
                dfValues = pd.DataFrame.from_dict(d['Review'].items())
                dfValues = dfValues.T   
                dfValues.columns = [s.replace("@", "") for s in list(d['Review'].keys())]

                if (i == 0):
                    df = dfValues.iloc[[1]]
                else:
                    tempDf = dfValues.iloc[[1]]
                    df = df.append(tempDf)

                i += 1

            df.insert(0,'AcctTxnLogId',tnxId)
            dfReviews = dfReviews.append(df)
        except KeyError:
            return dfReviews            

    return dfReviews

def processAttributes(d,dfAttributes,tnxId):
    logging.info('processAttributes...' + str(tnxId))

    try:
        d = d['Attributes']['Attributes']

        if d:
            i = 0

            for a in d:
                dfValues = pd.DataFrame.from_dict(d[i].items())
                dfValues = dfValues.T
                dfValues.columns = [s.replace("@", "") for s in list(d[i].keys())]

                if (i == 0):
                    df = dfValues.iloc[[1]]
                else:
                    tempDf = dfValues.iloc[[1]]
                    df.insert(0,str(tempDf.columns[0]),str(tempDf.iat[0,0]),allow_duplicates=False)

                i += 1

            df = df.sort_index(axis=1, ascending=True)
            df.insert(0,'AcctTxnLogId',tnxId)
            dfAttributes = dfAttributes.append(df)
    except KeyError:
        return dfAttributes          

    return dfAttributes

def processGeolocationInfo(d,dfGeolocationInfo,tnxId):
    try:
        d = d['GeolocationInfo']
        i = 0

        for a in d: 
            dfValues = pd.DataFrame.from_dict(d['GeolocationInfo'].items())
            dfValues = dfValues.T 
            dfValues.columns = [s.replace("@", "") for s in list(d['GeolocationInfo'].keys())]

            if (i == 0):
                df = dfValues.iloc[[1]]
            else:
                tempDf = dfValues.iloc[[1]]
                df = df.append(tempDf)
            i += 1

        df.insert(0,'AcctTxnLogId',tnxId)
        dfGeolocationInfo = dfGeolocationInfo.append(df)    

    except KeyError:
        return dfGeolocationInfo

    return dfGeolocationInfo  

def initializeDfAttributes(xmlDict,dfAttributes):
    try:
        d = xmlDict['Txns']['Txn'][0]['Attributes']['Attributes']
        i = 0
        attribList = []

        for a in d:        
            attribList.append(list(d[i].keys())[0])
            i += 1

        attribList = [s.replace("@", "") for s in attribList]
        attribList.sort(reverse=True)
        attribList.append('AcctTxnLogId')
        dfAttributes = pd.DataFrame(columns = attribList,dtype=object)
        dfAttributes = dfAttributes.sort_index(axis=1, ascending=True)
    except:
        return dfAttributes        

    return dfAttributes       

def initializeDfAttributes2(xmlDict,dfAttributes):      
    return dfAttributes

def initializeDfAlerts(xmlDict,dfAlerts):
    try:
        cols = [s.replace("@", "") for s in list(xmlDict['Txns']['Txn'][0]['Alerts']['Alerts'][0].keys())]
        dfAlerts =  pd.DataFrame(columns = cols.append('AcctTxnLogId'),dtype=object)
    except:
        cols = [s.replace("@", "") for s in list(xmlDict['Txns']['Txn'][1]['Alerts']['Alerts'][0].keys())]
        dfAlerts =  pd.DataFrame(columns = cols.append('AcctTxnLogId'),dtype=object)

    return dfAlerts  

def initializeDfRisks(xmlDict,dfRisks):
    d = xmlDict['Txns']['Txn'][0]

    if d['Risks']:
        try:
            cols = [s.replace("@", "") for s in list(d['Risks']['Risks'][0].keys())]
            dfRisks =  pd.DataFrame(columns = cols.append('AcctTxnLogId'),dtype=object)

        except KeyError:
            key = '@DepositRiskFactorId'

            if key in d['Risks']['Risks']:
                cols = [s.replace("@", "") for s in list(d['Risks']['Risks'].keys())]
                dfRisks =  pd.DataFrame(columns = cols.append('AcctTxnLogId'),dtype=object)
            #else:
            #    logging.info('No key...')

    return dfRisks

def initializeDfGeolocationInfo(xmlDict,dfGeolocationInfo):
    try:
        cols = [s.replace("@", "") for s in list(xmlDict['Txns']['Txn'][0]['GeolocationInfo']['GeolocationInfo'].keys())]
        dfGeolocationInfo = pd.DataFrame(columns = cols.append('AcctTxnLogId'),dtype=object)    
    except:
        cols = [s.replace("@", "") for s in list(xmlDict['Txns']['Txn'][1]['GeolocationInfo']['GeolocationInfo'].keys())]
        dfGeolocationInfo = pd.DataFrame(columns = cols.append('AcctTxnLogId'),dtype=object)

    return dfGeolocationInfo

def initializeDfReviews(xmlDict,dfReviews):
    try:
        xmlDict = xmlDict['Txns']['Txn'][0]['Reviews']
        key = '@DepositItemReviewLogId'
    
        if key in xmlDict['Review']:
            cols = [s.replace("@", "") for s in list(xmlDict['Review'].keys())] 
            dfRisks = pd.DataFrame(columns = cols.append('AcctTxnLogId'),dtype=object)  
    except:
        xmlDict = xmlDict['Txns']['Txn'][1]['Reviews']
        key = '@DepositItemReviewLogId'
    
        if key in xmlDict['Review']:
            cols = [s.replace("@", "") for s in list(xmlDict['Review'].keys())] 
            dfRisks = pd.DataFrame(columns = cols.append('AcctTxnLogId'),dtype=object)  

    return dfReviews

def initializeDfTransactions(xmlDict,dfTransactions):
    cols = [s.replace("@", "") for s in list(xmlDict['Txns']['Txn'][0].keys())]
    dfTransactions = pd.DataFrame(columns = cols.append('AcctTxnLogId'),dtype=object)  
    #dfTransactions.drop(['Attributes','Alerts','GeolocationInfo','Risks','Reviews'], axis = 1, inplace = True)

    return dfTransactions  

def getTransId(transaction):
    cols = [s.replace("@", "") for s in list(transaction.keys())]
    tempTransDf = pd.DataFrame.from_dict(transaction.items())
    tempTransDf = tempTransDf.T
    tempTransDf.columns = cols
    tempTransDf = tempTransDf.iloc[[1]]
    #tempTransDf.drop(['Attributes','Alerts','GeolocationInfo','Risks','Reviews'], axis = 1, inplace = True)
    accountTxnLogId = tempTransDf.iloc[0]['AcctTxnLogId']
    return accountTxnLogId
 
def processKitchenSink(lob,tag,blobName):
    logging.info('Processing Kitchen Sink..')

    fileName = str(blobName)
    pos = fileName.rfind(".")
    fileName = fileName[:pos]

    pos = fileName.rfind("/")
    fileName = fileName[pos+1:]

    logging.info('FileName = ' + fileName)

    if (lob == 'TMM'):
        #logging.info('lob=TMM')
        
        downloaded_blob = container_client.download_blob(blobName)
        #xml_data = open(blobName, 'r').read()  # Read data
        #xmlDict = xmltodict.parse(xml_data)  # Parse XML
        #logging.info('Downloaded Blob...')
        xmlDict = xmltodict.parse(downloaded_blob.content_as_text())
    
        #logging.info('Initialization...')

        dfAlerts = initializeDfAlerts(xmlDict,pd.DataFrame())
        logging.info('Alerts...')
        dfAttributes = initializeDfAttributes2(xmlDict,pd.DataFrame())
        logging.info('Attributes...')
        dfRisks = initializeDfRisks(xmlDict,pd.DataFrame())
        logging.info('Risks...')
        dfReviews = initializeDfReviews(xmlDict,pd.DataFrame())
        logging.info('Reviews...')
        dfGeolocationInfo = initializeDfGeolocationInfo(xmlDict,pd.DataFrame())
        logging.info('Initialized...')
        
        try:
            logging.info('Inside Try...')
            index = 0
            txnDict = xmlDict['Txns']['Txn']     


            logging.info('Tags...')

            if (tag == 'Transactions'):
                dfTransactions = initializeDfTransactions(xmlDict,pd.DataFrame())

                for d in txnDict:
                    transaction = txnDict[index]
                    cols = [s.replace("@", "") for s in list(transaction.keys())]
                    tempTransDf = pd.DataFrame.from_dict(transaction.items())
                    tempTransDf = tempTransDf.T
                    tempTransDf.columns = cols
                    tempTransDf = tempTransDf.iloc[[1]]
                    #tempTransDf.drop(['Attributes','Alerts','GeolocationInfo','Risks','Reviews'], axis = 1, inplace = True)
                    dfTransactions = dfTransactions.append(tempTransDf)
                    index += 1


                dfTransactions.drop(['Attributes','Alerts','GeolocationInfo','Risks','Reviews'], axis = 1, inplace = True)
                blobName = "/out/tmm/" + fileName + "-transactions.csv"
                container_client.upload_blob(blobName,dfTransactions.to_csv(index=False),'BlockBlob',overwrite=True)
                logging.info('Processed Transactions...'+dfTransactions)
            elif (tag == 'Alerts'):
                for d in txnDict:
                    transaction = txnDict[index]
                    dfAlerts = processAlerts(transaction,dfAlerts,getTransId(transaction))
                    index += 1

                blobName = "/out/tmm/" + fileName + "-alerts.csv"
                container_client.upload_blob(blobName,dfAlerts.to_csv(index=False),'BlockBlob',overwrite=True)      
                logging.info('Processed Alerts...'+dfAlerts)
            elif (tag == 'Risks'):
                for d in txnDict:
                    transaction = txnDict[index]
                    dfRisks = processRisks(transaction,dfRisks,getTransId(transaction))
                    index += 1

                blobName = "/out/tmm/" + fileName + "-risks.csv"
                container_client.upload_blob(blobName,dfRisks.to_csv(index=False),'BlockBlob',overwrite=True)       
                logging.info('Processed Risks...' + dfRisks) 
            elif (tag == 'Reviews'):
                logging.info('tag = Reviews')

                for d in txnDict:
                    transaction = txnDict[index]
                    dfReviews = processReviews(transaction,dfReviews,getTransId(transaction))
                    index += 1

                blobName = "/out/tmm/" + fileName + "-reviews.csv"
                container_client.upload_blob(blobName,dfReviews.to_csv(index=False),'BlockBlob',overwrite=True)  
                logging.info('Processed Reviews...' + dfReviews) 
            elif (tag == 'GeolocationInfo'):
                for d in txnDict:
                    transaction = txnDict[index]
                    dfGeolocationInfo = processGeolocationInfo(transaction,dfGeolocationInfo,getTransId(transaction))
                    index += 1

                blobName = "/out/tmm/" + fileName + "-geolocationInfo.csv"
                container_client.upload_blob(blobName,dfGeolocationInfo.to_csv(index=False),'BlockBlob',overwrite=True)    
                logging.info('Processed GeolocationInfo...' + dfGeolocationInfo)
            elif (tag == 'Attributes'):
                for d in txnDict:
                    transaction = txnDict[index]
                    dfAttributes = processAttributes(transaction,dfAttributes,getTransId(transaction))
                    index += 1

                blobName = "/out/tmm/" + fileName + "-attributes.csv"
                container_client.upload_blob(blobName,dfAttributes.to_csv(index=False),'BlockBlob',overwrite=True)    
                logging.info('Processed Attributes...' + dfAttributes)                                            

        except Exception as e: # work on python 3.x
            logging.info('error: ' + str(e))

    if (lob == 'VIBE'):
        outBlobPrefix = "/out/vibe/"
        #logging.info('lob=TMM')
        
        downloaded_blob = container_client.download_blob(blobName)
        #xml_data = open(blobName, 'r').read()  # Read data
        #xmlDict = xmltodict.parse(xml_data)  # Parse XML
        #logging.info('Downloaded Blob...')
        xmlDict = xmltodict.parse(downloaded_blob.content_as_text())
    
        #logging.info('Initialization...')

        dfAlerts = initializeDfAlerts(xmlDict,pd.DataFrame())
        logging.info('Alerts...')
        dfAttributes = initializeDfAttributes2(xmlDict,pd.DataFrame())
        logging.info('Attributes...')
        dfRisks = initializeDfRisks(xmlDict,pd.DataFrame())
        logging.info('Risks...')
        dfReviews = initializeDfReviews(xmlDict,pd.DataFrame())
        logging.info('Reviews...')
        dfGeolocationInfo = initializeDfGeolocationInfo(xmlDict,pd.DataFrame())
        logging.info('Initialized...')
        
        try:
            logging.info('Inside Try...')
            index = 0
            txnDict = xmlDict['Txns']['Txn']     


            logging.info('Tags...')

            if (tag == 'Transactions'):
                dfTransactions = initializeDfTransactions(xmlDict,pd.DataFrame())

                for d in txnDict:
                    transaction = txnDict[index]
                    cols = [s.replace("@", "") for s in list(transaction.keys())]
                    tempTransDf = pd.DataFrame.from_dict(transaction.items())
                    tempTransDf = tempTransDf.T
                    tempTransDf.columns = cols
                    tempTransDf = tempTransDf.iloc[[1]]
                    #tempTransDf.drop(['Attributes','Alerts','GeolocationInfo','Risks','Reviews'], axis = 1, inplace = True)
                    dfTransactions = dfTransactions.append(tempTransDf)
                    index += 1

                blobName = outBlobPrefix + fileName + "-transactions.csv"
                container_client.upload_blob(blobName,dfTransactions.to_csv(index=False),'BlockBlob',overwrite=True)
                logging.info('Processed Transactions...'+dfTransactions)
            elif (tag == 'Alerts'):
                for d in txnDict:
                    transaction = txnDict[index]
                    dfAlerts = processAlerts(transaction,dfAlerts,getTransId(transaction))
                    index += 1

                blobName = outBlobPrefix + fileName + "-alerts.csv"
                container_client.upload_blob(blobName,dfAlerts.to_csv(index=False),'BlockBlob',overwrite=True)      
                logging.info('Processed Alerts...'+dfAlerts)
            elif (tag == 'Risks'):
                for d in txnDict:
                    transaction = txnDict[index]
                    dfRisks = processRisks(transaction,dfRisks,getTransId(transaction))
                    index += 1

                blobName = outBlobPrefix + fileName + "-risks.csv"
                container_client.upload_blob(blobName,dfRisks.to_csv(index=False),'BlockBlob',overwrite=True)       
                logging.info('Processed Risks...' + dfRisks) 
            elif (tag == 'Reviews'):
                logging.info('tag = Reviews')

                for d in txnDict:
                    transaction = txnDict[index]
                    dfReviews = processReviews(transaction,dfReviews,getTransId(transaction))
                    index += 1

                blobName = outBlobPrefix + fileName + "-reviews.csv"
                container_client.upload_blob(blobName,dfReviews.to_csv(index=False),'BlockBlob',overwrite=True)  
                logging.info('Processed Reviews...' + dfReviews) 
            elif (tag == 'GeolocationInfo'):
                for d in txnDict:
                    transaction = txnDict[index]
                    dfGeolocationInfo = processGeolocationInfo(transaction,dfGeolocationInfo,getTransId(transaction))
                    index += 1

                blobName = outBlobPrefix + fileName + "-geolocationInfo.csv"
                container_client.upload_blob(blobName,dfGeolocationInfo.to_csv(index=False),'BlockBlob',overwrite=True)    
                logging.info('Processed GeolocationInfo...' + dfGeolocationInfo)
            elif (tag == 'Attributes'):
                for d in txnDict:
                    transaction = txnDict[index]
                    dfAttributes = processAttributes(transaction,dfAttributes,getTransId(transaction))
                    index += 1

                blobName = outBlobPrefix + fileName + "-attributes.csv"
                container_client.upload_blob(blobName,dfAttributes.to_csv(index=False),'BlockBlob',overwrite=True)    
                logging.info('Processed Attributes...' + dfAttributes)                                            

        except Exception as e: # work on python 3.x
            logging.info('error: ' + str(e))

        
#logging.info("HELLO")
#processKitchenSink('TMM','GeolocationInfo','BankMobileCobalt-MBL-XML20210603163322-4665732.xml')        