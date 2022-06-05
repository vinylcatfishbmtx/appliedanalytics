from distutils.log import error
import azure.functions as func
from azure.storage.blob import ContainerClient
from numpy import datetime_as_string
import pandas as pd
import logging
import xmltodict

class KitchenSinkXmlTransformer:

    def __init__(self):
        self.name = "KitchenSinkXmlTransformer"

    def getName(self):
        return self.name    

    def processAlerts(self,d,dfAlerts,tnxId):
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

    def processRisks(self,d,dfRisks,tnxId):
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

    def processReviews(self,d,dfReviews,tnxId):
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

    def processAttributes(self,d,dfAttributes,tnxId):
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

    def processGeolocationInfo(self,d,dfGeolocationInfo,tnxId):
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

    def initializeDfAttributes(self,xmlDict,dfAttributes):
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

    def initializeDfAttributes2(self,xmlDict,dfAttributes):      
        return dfAttributes

    def initializeDfAlerts(self,xmlDict,dfAlerts):
        try:
            cols = [s.replace("@", "") for s in list(xmlDict['Txns']['Txn'][0]['Alerts']['Alerts'][0].keys())]
            dfAlerts =  pd.DataFrame(columns = cols.append('AcctTxnLogId'),dtype=object)
        except:
            cols = [s.replace("@", "") for s in list(xmlDict['Txns']['Txn'][1]['Alerts']['Alerts'][0].keys())]
            dfAlerts =  pd.DataFrame(columns = cols.append('AcctTxnLogId'),dtype=object)

        return dfAlerts  

    def initializeDfRisks(self,xmlDict,dfRisks):
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

        return dfRisks

    def initializeDfGeolocationInfo(self,xmlDict,dfGeolocationInfo):
        try:
            cols = [s.replace("@", "") for s in list(xmlDict['Txns']['Txn'][0]['GeolocationInfo']['GeolocationInfo'].keys())]
            dfGeolocationInfo = pd.DataFrame(columns = cols.append('AcctTxnLogId'),dtype=object)    
        except:
            cols = [s.replace("@", "") for s in list(xmlDict['Txns']['Txn'][1]['GeolocationInfo']['GeolocationInfo'].keys())]
            dfGeolocationInfo = pd.DataFrame(columns = cols.append('AcctTxnLogId'),dtype=object)

        return dfGeolocationInfo

    def initializeDfReviews(self,xmlDict,dfReviews):
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

    def initializeDfTransactions(self,xmlDict,dfTransactions):
        cols = [s.replace("@", "") for s in list(xmlDict['Txns']['Txn'][0].keys())]
        dfTransactions = pd.DataFrame(columns = cols.append('AcctTxnLogId'),dtype=object)  
        #dfTransactions.drop(['Attributes','Alerts','GeolocationInfo','Risks','Reviews'], axis = 1, inplace = True)

        return dfTransactions  

    def getTransId(self,transaction):
        cols = [s.replace("@", "") for s in list(transaction.keys())]
        tempTransDf = pd.DataFrame.from_dict(transaction.items())
        tempTransDf = tempTransDf.T
        tempTransDf.columns = cols
        tempTransDf = tempTransDf.iloc[[1]]
        #tempTransDf.drop(['Attributes','Alerts','GeolocationInfo','Risks','Reviews'], axis = 1, inplace = True)
        accountTxnLogId = tempTransDf.iloc[0]['AcctTxnLogId']
        return accountTxnLogId

    def processKitchenSink(self,lob,tag,blobName,container_client):
        logging.info('Processing Kitchen Sink..')

        fileName = str(blobName)
        pos = fileName.rfind(".")
        fileName = fileName[:pos]

        pos = fileName.rfind("/")
        fileName = fileName[pos+1:]

        logging.info('FileName = ' + fileName)

        if (lob == 'TMM'):
            downloaded_blob = container_client.download_blob(blobName)
            xmlDict = xmltodict.parse(downloaded_blob.content_as_text())

            dfAlerts = self.initializeDfAlerts(xmlDict,pd.DataFrame())
            dfAttributes = self.initializeDfAttributes2(xmlDict,pd.DataFrame())
            dfRisks = self.initializeDfRisks(xmlDict,pd.DataFrame())
            dfReviews = self.initializeDfReviews(xmlDict,pd.DataFrame())
            dfGeolocationInfo = self.initializeDfGeolocationInfo(xmlDict,pd.DataFrame())
            
            try:
                index = 0
                txnDict = xmlDict['Txns']['Txn']     

                if (tag == 'Transactions'):
                    dfTransactions = self.initializeDfTransactions(xmlDict,pd.DataFrame())

                    for d in txnDict:
                        transaction = txnDict[index]
                        cols = [s.replace("@", "") for s in list(transaction.keys())]
                        tempTransDf = pd.DataFrame.from_dict(transaction.items())
                        tempTransDf = tempTransDf.T
                        tempTransDf.columns = cols
                        tempTransDf = tempTransDf.iloc[[1]]
                        dfTransactions = dfTransactions.append(tempTransDf)
                        index += 1

                    dfTransactions.drop(['Attributes','Alerts','GeolocationInfo','Risks','Reviews'], axis = 1, inplace = True)
                    blobName = "/out/tmm/" + fileName + "-transactions.csv"
                    container_client.upload_blob(blobName,dfTransactions.to_csv(index=False),'BlockBlob',overwrite=True)
                    logging.info('Processed Transactions...'+dfTransactions)
                elif (tag == 'Alerts'):
                    for d in txnDict:
                        transaction = txnDict[index]
                        dfAlerts = self.processAlerts(transaction,dfAlerts,self.getTransId(transaction))
                        index += 1

                    blobName = "/out/tmm/" + fileName + "-alerts.csv"
                    container_client.upload_blob(blobName,dfAlerts.to_csv(index=False),'BlockBlob',overwrite=True)      
                    logging.info('Processed Alerts...'+dfAlerts)
                elif (tag == 'Risks'):
                    for d in txnDict:
                        transaction = txnDict[index]
                        dfRisks = self.processRisks(transaction,dfRisks,self.getTransId(transaction))
                        index += 1

                    blobName = "/out/tmm/" + fileName + "-risks.csv"
                    container_client.upload_blob(blobName,dfRisks.to_csv(index=False),'BlockBlob',overwrite=True)       
                    logging.info('Processed Risks...' + dfRisks) 
                elif (tag == 'Reviews'):
                    logging.info('tag = Reviews')

                    for d in txnDict:
                        transaction = txnDict[index]
                        dfReviews = self.processReviews(transaction,dfReviews,self.getTransId(transaction))
                        index += 1

                    blobName = "/out/tmm/" + fileName + "-reviews.csv"
                    container_client.upload_blob(blobName,dfReviews.to_csv(index=False),'BlockBlob',overwrite=True)  
                    logging.info('Processed Reviews...' + dfReviews) 
                elif (tag == 'GeolocationInfo'):
                    for d in txnDict:
                        transaction = txnDict[index]
                        dfGeolocationInfo = self.processGeolocationInfo(transaction,dfGeolocationInfo,self.getTransId(transaction))
                        index += 1

                    blobName = "/out/tmm/" + fileName + "-geolocationInfo.csv"
                    container_client.upload_blob(blobName,dfGeolocationInfo.to_csv(index=False),'BlockBlob',overwrite=True)    
                    logging.info('Processed GeolocationInfo...' + dfGeolocationInfo)
                elif (tag == 'Attributes'):
                    for d in txnDict:
                        transaction = txnDict[index]
                        dfAttributes = self.processAttributes(transaction,dfAttributes,self.getTransId(transaction))
                        index += 1

                    blobName = "/out/tmm/" + fileName + "-attributes.csv"
                    container_client.upload_blob(blobName,dfAttributes.to_csv(index=False),'BlockBlob',overwrite=True)    
                    logging.info('Processed Attributes...' + dfAttributes)                                            

            except Exception as e: # work on python 3.x
                logging.info('error: ' + str(e))

        if (lob == 'VIBE'):
            outBlobPrefix = "/out/vibe/"
            downloaded_blob = container_client.download_blob(blobName)
            xmlDict = xmltodict.parse(downloaded_blob.content_as_text())
        
            dfAlerts = self.initializeDfAlerts(xmlDict,pd.DataFrame())
            dfAttributes = self.initializeDfAttributes2(xmlDict,pd.DataFrame())
            dfRisks = self.initializeDfRisks(xmlDict,pd.DataFrame())
            dfReviews = self.initializeDfReviews(xmlDict,pd.DataFrame())
            dfGeolocationInfo = self.initializeDfGeolocationInfo(xmlDict,pd.DataFrame())
            
            try:
                index = 0
                txnDict = xmlDict['Txns']['Txn']     

                if (tag == 'Transactions'):
                    dfTransactions = self.initializeDfTransactions(xmlDict,pd.DataFrame())

                    for d in txnDict:
                        transaction = txnDict[index]
                        cols = [s.replace("@", "") for s in list(transaction.keys())]
                        tempTransDf = pd.DataFrame.from_dict(transaction.items())
                        tempTransDf = tempTransDf.T
                        tempTransDf.columns = cols
                        tempTransDf = tempTransDf.iloc[[1]]
                        dfTransactions = dfTransactions.append(tempTransDf)
                        index += 1

                    blobName = outBlobPrefix + fileName + "-transactions.csv"
                    container_client.upload_blob(blobName,dfTransactions.to_csv(index=False),'BlockBlob',overwrite=True)
                    logging.info('Processed Transactions...'+dfTransactions)
                elif (tag == 'Alerts'):    
                    for d in txnDict:
                        transaction = txnDict[index]
                        dfAlerts = self.processAlerts(transaction,dfAlerts,self.getTransId(transaction))
                        self.processAlerts(transaction,dfAlerts,self.getTransId(transaction))
                        index += 1

                    blobName = outBlobPrefix + fileName + "-alerts.csv"
                    container_client.upload_blob(blobName,dfAlerts.to_csv(index=False),'BlockBlob',overwrite=True)      
                    logging.info('Processed Alerts...'+dfAlerts)
                elif (tag == 'Risks'):
                    for d in txnDict:
                        transaction = txnDict[index]
                        dfRisks = self.processRisks(transaction,dfRisks,self.getTransId(transaction))
                        index += 1

                    blobName = outBlobPrefix + fileName + "-risks.csv"
                    container_client.upload_blob(blobName,dfRisks.to_csv(index=False),'BlockBlob',overwrite=True)       
                    logging.info('Processed Risks...' + dfRisks) 
                elif (tag == 'Reviews'):
                    logging.info('tag = Reviews')

                    for d in txnDict:
                        transaction = txnDict[index]
                        dfReviews = self.processReviews(transaction,dfReviews,self.getTransId(transaction))
                        index += 1

                    blobName = outBlobPrefix + fileName + "-reviews.csv"
                    container_client.upload_blob(blobName,dfReviews.to_csv(index=False),'BlockBlob',overwrite=True)  
                    logging.info('Processed Reviews...' + dfReviews) 
                elif (tag == 'GeolocationInfo'):
                    for d in txnDict:
                        transaction = txnDict[index]
                        dfGeolocationInfo = self.processGeolocationInfo(transaction,dfGeolocationInfo,self.getTransId(transaction))
                        index += 1

                    blobName = outBlobPrefix + fileName + "-geolocationInfo.csv"
                    container_client.upload_blob(blobName,dfGeolocationInfo.to_csv(index=False),'BlockBlob',overwrite=True)    
                    logging.info('Processed GeolocationInfo...' + dfGeolocationInfo)
                elif (tag == 'Attributes'):
                    for d in txnDict:
                        transaction = txnDict[index]
                        dfAttributes = self.processAttributes(transaction,dfAttributes,self.getTransId(transaction))
                        index += 1

                    blobName = outBlobPrefix + fileName + "-attributes.csv"
                    container_client.upload_blob(blobName,dfAttributes.to_csv(index=False),'BlockBlob',overwrite=True)    
                    logging.info('Processed Attributes...' + dfAttributes)                                            

            except Exception as e: # work on python 3.x
                logging.info('error: ' + str(e))
    


#def initializeDataFrames():    

#x = KitchenSinkXmlTransformer("TMM")