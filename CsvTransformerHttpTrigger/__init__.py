import azure.functions as func
from azure.storage.blob import ContainerClient
from numpy import datetime_as_string
import pandas as pd
import datetime
import logging
from io import StringIO

dateStr = str(datetime.datetime.now().strftime("%Y-%m-%d"))
conn_str = "DefaultEndpointsProtocol=https;AccountName=adldataopscentralus;AccountKey=DYJ1kZ3IrsLbBMlNDmgIWHkSoK36GHjUumhnb2w4H0kUbVpzs4cCfrQ6Z9gf2kINHz9AtdzVkswKBosdryIGpg==;EndpointSuffix=core.windows.net"
container_str = "teradatatmmaggregatefeeds"
container_client = ContainerClient.from_connection_string(
    conn_str=conn_str, 
    container_name=container_str
)

account_type_codes = [320, 460]
transaction_type = ['deposit', 'spend']
account_types = ((320, 'checking'), (460, 'savings'))  
flag_columns = [
        'Primary Account Holder Flag',
        'Wireless Flag',
        'Postpaid Flag', 
        'Eligible Flag']
report_types = ['transactions', 'balances']        

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if name:
        if (name == "DailyReports"):
            getDailyReports()
                
        return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )

def getDailyReports():
    data = dateStr
    logBlobName = "/out/log/DailyReports.txt"
    
    #---------------------------------------------------------------------------------------------------
    #Transactions Reports
    #Download In Blob
    blobName = "/in/daily/tmmtransactionsfeeddata.csv"
    downloaded_blob = container_client.download_blob(blobName)
    #Convert to Pandas Dataframe
    df = pd.read_csv(StringIO(downloaded_blob.content_as_text()))
    shapeData(df)

    #note: conditional log determines which columns are included in given list.  this is key to what columns show up on the report!
    spend_columns = [i for i in df.columns.to_list() if 'Withdrawal' in i]
    deposit_columns = [i for i in df.columns.to_list() if 'Deposit' in i]
    percentage_columns = [i for i in df.columns.to_list() if 'Percentage' in i]
    balance_columns = [i for i in df.columns.to_list() if 'Balance' in i]
    open_columns = [i for i in df.columns.to_list() if 'Open' in i]

    getFilteredReports(df,spend_columns,deposit_columns,percentage_columns,balance_columns,open_columns)
    getNonFilteredReports(df,spend_columns,deposit_columns,balance_columns,open_columns)

    #---------------------------------------------------------------------------------------------------
    #New Account Opening Reports
    #Download In Blob

    newAccountsBlobName = "/in/daily/tmmnewaccountsfeeddata.csv"
    downloaded_blob = container_client.download_blob(newAccountsBlobName)
    
    #Convert to Pandas Dataframe
    
    df = pd.read_csv(StringIO(downloaded_blob.content_as_text()))
    shapeAccountOpeningData(df)

    deposit_columns = [i for i in df.columns.to_list() if 'Deposit' in i]
    
    getFilteredAccountOpeningReports(df,deposit_columns)
    getNonFilteredAccountOpeningReports(df,deposit_columns)
    
    #Write Date to Log File
    container_client.upload_blob(name=logBlobName,data=data,blob_type='BlockBlob',overwrite=True)

def getFilteredReports(df,spend_columns,deposit_columns,percentage_columns,balance_columns,open_columns):
    moneyMovementOutCheckingBlobName = "Money_Movement_Out_with_filters_Daily_" + dateStr + ".csv"
    moneyMovementOutSavingsBlobName = "Savings_Money_Movement_Out_with_filters_Daily_"+ dateStr + ".csv"
    depositDailyCheckingBlobName = "Deposit_Activity_with_filters_Daily_" + dateStr + ".csv"
    depositDailySavingsBlobName = "Savings_Deposit_Activity_with_filters_Daily_"+ dateStr + ".csv"
    accountBalanceDailyCheckingBlobName = "Account_Balance_Data_with_filters_Daily_" + dateStr + ".csv"
    accountBalanceDailySavingsBlobName = "Savings_Account_Balance_Data_with_filters_Daily_"+ dateStr + ".csv"

    for r in report_types:
        for a,b in account_types:
            for t in transaction_type:
                with_filters_report = df.copy()
                with_filters_report = with_filters_report[with_filters_report['Account Type Code'] == a] # filter on checking or savings. we don't have savings data yet, so I'm just doing checking for now.
                with_filters_report['Post Date'] = pd.to_datetime(with_filters_report['Post Date']) # filter df on max date in dataset
                with_filters_report = with_filters_report[with_filters_report['Post Date'] == with_filters_report['Post Date'].max()] 
                with_filters_report.drop(columns=['Account Type Code', 'Post Date'], inplace=True)
                with_filters_report = pd.pivot_table(with_filters_report, index=['Primary Account Holder Flag', 'Wireless Flag', 'Postpaid Flag', 'Eligible Flag'], fill_value=0)
                with_filters_report.reset_index(inplace=True)
                with_filters_report[with_filters_report.columns[~with_filters_report.columns.isin(percentage_columns)]] = with_filters_report[with_filters_report.columns[~with_filters_report.columns.isin(percentage_columns)]].round(0) # round all values except for % column

                if r == 'transactions':
                    if t == 'spend':
                        with_filters_report = with_filters_report.reindex(columns=flag_columns+spend_columns) # reorder columns bc pivot table flips them
                        if b == 'checking':
                            container_client.upload_blob(moneyMovementOutCheckingBlobName,with_filters_report.to_csv(),'BlockBlob',overwrite=True)
                        elif b == 'savings':
                            container_client.upload_blob(moneyMovementOutSavingsBlobName,with_filters_report.to_csv(),'BlockBlob',overwrite=True)
                    elif t == 'deposit':
                        with_filters_report = with_filters_report.reindex(columns=flag_columns+deposit_columns) # reorder columns bc pivot table flips them
                        if b == 'checking':
                            container_client.upload_blob(depositDailyCheckingBlobName,with_filters_report.to_csv(),'BlockBlob',overwrite=True)
                        elif b == 'savings':
                            container_client.upload_blob(depositDailySavingsBlobName,with_filters_report.to_csv(),'BlockBlob',overwrite=True)
                elif r == 'balances':
                    with_filters_report = with_filters_report.reindex(columns=flag_columns+balance_columns+open_columns)
                    if b == 'checking':
                        container_client.upload_blob(accountBalanceDailyCheckingBlobName,with_filters_report.to_csv(),'BlockBlob',overwrite=True)
                    if b == 'savings':
                        container_client.upload_blob(accountBalanceDailySavingsBlobName,with_filters_report.to_csv(),'BlockBlob',overwrite=True)


def getNonFilteredReports(df,spend_columns,deposit_columns,balance_columns,open_columns):
    moneyMovementOutCheckingBlobName = "Money_Movement_Out_Daily_" + dateStr + ".csv"
    moneyMovementOutSavingsBlobName = "Savings_Money_Movement_Out_Daily_"+ dateStr + ".csv"
    depositDailyCheckingBlobName = "Deposit_Activity_Daily_" + dateStr + ".csv"
    depositDailySavingsBlobName = "Savings_Deposit_Activity_Daily_"+ dateStr + ".csv"
    accountBalanceDailyCheckingBlobName = "Account_Balance_Data_Daily_" + dateStr + ".csv"
    accountBalanceDailySavingsBlobName = "Savings_Account_Balance_Data_Daily_"+ dateStr + ".csv"

    for r in report_types:
        for a,b in account_types:
            for t in transaction_type:
            
                df_no_filters = df.copy()
                df_no_filters = df_no_filters[df_no_filters['Account Type Code'] == a] # filter on checking or savings. we don't have savings data yet, so I'm just doing checking for now.
                df_no_filters.drop(columns=flag_columns + ['Account Type Code'], inplace=True)
                df_no_filters = df_no_filters.groupby('Post Date').sum().reset_index() # need to aggregate information because it was broken out by the flags
                df_no_filters['Post Date'] = df_no_filters['Post Date'].astype(str) # change date to string type
                
                if len(df_no_filters) != 0:
                    df_no_filters.loc['Total']= df_no_filters.sum() # add total row
                    df_no_filters.at['Total','Post Date']='Total' # rename total row as "Total"

                if r == 'transactions':
                    # recalculate averages since we did the group by above (amt/n) as well as the %
                    if t == 'spend':
                        calculate_avgs('spend',df_no_filters)
                    elif t == 'deposit':
                        calculate_avgs('deposit',df_no_filters)

                    df_no_filters.drop(columns=['Number of Open T-Mobile Savings Accounts'], inplace=True)
                    table = pd.pivot_table(df_no_filters, columns=['Post Date'], fill_value=0)

                    if t == 'spend':
                        table = table.reindex(spend_columns, axis=0) # reorder columns bc pivot table flips them
                        if b == 'checking':
                            container_client.upload_blob(moneyMovementOutCheckingBlobName,table.to_csv(),'BlockBlob',overwrite=True)
                        elif b == 'savings':
                            container_client.upload_blob(moneyMovementOutSavingsBlobName,table.to_csv(),'BlockBlob',overwrite=True)
                    elif t == 'deposit':
                        table = table.reindex(deposit_columns, axis=0) # reorder columns bc pivot table flips them
                        if b == 'checking':
                            container_client.upload_blob(depositDailyCheckingBlobName,table.to_csv(),'BlockBlob',overwrite=True)
                        elif b == 'savings':
                            container_client.upload_blob(depositDailySavingsBlobName,table.to_csv(),'BlockBlob',overwrite=True)
                elif r == 'balances':
                    calculate_avgs('balances',df_no_filters)
                    table = pd.pivot_table(df_no_filters, columns=['Post Date'], fill_value=0)
                    table = table.reindex(balance_columns+open_columns, axis=0) # reorder columns bc pivot table flips them
                    if b == 'checking':
                        container_client.upload_blob(accountBalanceDailyCheckingBlobName,table.to_csv(),'BlockBlob',overwrite=True)
                    elif b == 'savings':
                        container_client.upload_blob(accountBalanceDailySavingsBlobName,table.to_csv(),'BlockBlob',overwrite=True)

def getFilteredAccountOpeningReports(df,deposit_columns):
    accountOpeningDailyCheckingBlobName = "New_Account_Opening_Deposits_with_filters_Daily_" + dateStr + ".csv"
    accountOpeningDailySavingsBlobName = "Savings_New_Account_Opening_Deposits_with_filters_Daily_"+ dateStr + ".csv"

    for a,b in account_types:
        with_filters_report = df.copy()
        with_filters_report = with_filters_report[with_filters_report['Account Type Code'] == a] # filter on checking or savings. we don't have savings data yet, so I'm just doing checking for now.
        with_filters_report['Post Date'] = pd.to_datetime(with_filters_report['Post Date']) # filter df on max date in dataset
        with_filters_report = with_filters_report[with_filters_report['Post Date'] == with_filters_report['Post Date'].max()] 
        with_filters_report.drop(columns=['Account Type Code', 'Post Date'], inplace=True)
        with_filters_report = pd.pivot_table(with_filters_report, index=['Primary Account Holder Flag', 'Wireless Flag', 'Postpaid Flag', 'Eligible Flag'], fill_value=0)
        with_filters_report.reset_index(inplace=True)
        with_filters_report[with_filters_report.columns] = with_filters_report[with_filters_report.columns].round(0) # round all values except for % column

        with_filters_report = with_filters_report.reindex(columns=flag_columns+deposit_columns) # reorder columns bc pivot table flips them
        if b == 'checking':
            container_client.upload_blob(accountOpeningDailyCheckingBlobName,with_filters_report.to_csv(),'BlockBlob',overwrite=True)
        elif b == 'savings':
            container_client.upload_blob(accountOpeningDailySavingsBlobName,with_filters_report.to_csv(),'BlockBlob',overwrite=True)

def getNonFilteredAccountOpeningReports(df,deposit_columns):
    accountOpeningDailyCheckingBlobName = "New_Account_Opening_Deposits_Daily_" + dateStr + ".csv"
    accountOpeningDailySavingsBlobName = "Savings_New_Account_Opening_Deposits_Daily_"+ dateStr + ".csv"

    for a,b in account_types:
        df_no_filters = df.copy()
        df_no_filters = df_no_filters[df_no_filters['Account Type Code'] == a] # filter on checking or savings. we don't have savings data yet, so I'm just doing checking for now.
        df_no_filters.drop(columns=flag_columns + ['Account Type Code'], inplace=True)
        df_no_filters = df_no_filters.groupby('Post Date').sum().reset_index() # need to aggregate information because it was broken out by the flags
        df_no_filters['Post Date'] = df_no_filters['Post Date'].astype(str) # change date to string type
        if len(df_no_filters) != 0:
            df_no_filters.loc['Total']= df_no_filters.sum() # add total row
            df_no_filters.at['Total','Post Date']='Total' # rename total row as "Total"

        # recalculate averages since we did the group by above (amt/n) as well as the %
        calculateAccountOpeningAvgs(df_no_filters)

        table = pd.pivot_table(df_no_filters, columns=['Post Date'], fill_value=0)
        table = table.reindex(deposit_columns, axis=0) # reorder columns bc pivot table flips them
        if b == 'checking':
            container_client.upload_blob(accountOpeningDailyCheckingBlobName,table.to_csv(),'BlockBlob',overwrite=True)
        elif b == 'savings':
            container_client.upload_blob(accountOpeningDailySavingsBlobName,table.to_csv(),'BlockBlob',overwrite=True)
                        
# function to averages and percents
def calculate_avgs(transaction_type,df):
    if transaction_type == 'spend':
        df['Average Withdrawal Amount from T-Mobile Savings Account'] = df['Amount via Withdrawal Made from T-Mobile Savings Accounts'] / df['Number of Withdrawals Made from T-Mobile Savings Accounts']
        df['Average Withdrawal Amount through ACH'] = df['Amount via ACH Withdrawal'] / df['Number of ACH Withdrawal']
        df['Average Withdrawal Amount through Paper Check'] = df['Amount via Paper Check Withdrawal'] / df['Number of Paper Check Withdrawal']
        df['Average Withdrawal Amount through Bill Pay'] = df['Amount via Bill Pay Withdrawal'] / df['Number of Bill Pay Withdrawal']
        df['Average Withdrawal Amount through ATM'] = df['Amount via ATM Withdrawal'] / df['Number of ATM Withdrawal']
        df['Average Withdrawal Amount through Debit Card'] = df['Amount via Debit Card Withdrawal'] / df['Number of Debit Card Withdrawal']
        df['Percentage of Accounts Making a Withdrawal'] = 100 * df['Number of T-Mobile Savings Accounts with Withdrawals'] / df['Number of Open T-Mobile Savings Accounts']

    elif transaction_type == 'deposit':
        df['Average Deposit Amount to T-Mobile Savings Account'] = df['Amount via Deposits Made to T-Mobile Savings Accounts']/df['Number of Deposits Made to T-Mobile Savings Accounts']
        df['Average Amount of Deposit by ACH'] = df['Amount via ACH Deposits']/df['Number of ACH Deposits']
        df['Average Amount of Deposit by RDC'] = df['Amount via RDC Deposits']/df['Number of RDC Deposits']
        df['Average Amount of Deposit by Check - (Mailed in)'] = df['Amount via Check - (Mailed in) Deposits']/df['Number of Check - (Mailed in) Deposits']
        df['Average Amount of Deposit by Wire'] = df['Amount via Wire Deposits']/df['Number of Wire Deposits']
        df['Percentage of Accounts Making a Deposit'] = 100 * df['Number of T-Mobile Savings Accounts with Deposit Transaction'] / df['Number of Open T-Mobile Savings Accounts']
        
    elif transaction_type == 'balance':
        df['Available Balance per T-Mobile Savings Account'] = df['Available Balance in T-Mobile Savings Accounts']/df['Number of Open T-Mobile Savings Accounts']
        df['Ledger Balance per T-Mobile Savings Account'] = df['Ledger Balance in T-Mobile Savings Accounts']/df['Number of Open T-Mobile Savings Accounts']


def shapeData(df):
    # rename columns per requirements
    df.rename(columns={"IS_WIRELESS_FLAG": "Wireless Flag", 
                   "IS_PRIMARY_ACCOUNT_HOLDER_FLAG": "Primary Account Holder Flag",
                   "IS_POSTPAID_FLAG": "Postpaid Flag",
                   "IS_ELIGIBLE_FLAG": "Eligible Flag",
                   "ACCOUNT_TYPE_CODE": "Account Type Code",
                   "POST_DATE": "Post Date",
                   "n_spend": "Number of Withdrawals Made from T-Mobile Savings Accounts",
                   "spend_amt": "Amount via Withdrawal Made from T-Mobile Savings Accounts",
                   "n_spend_ach": "Number of ACH Withdrawal",
                   "spend_ach_amt": "Amount via ACH Withdrawal",
                   "n_check": "Number of Paper Check Withdrawal",
                   "check_amt": "Amount via Paper Check Withdrawal",
                   "n_bill_pay": "Number of Bill Pay Withdrawal",
                   "bill_pay_amt": "Amount via Bill Pay Withdrawal",
                   "n_atm": "Number of ATM Withdrawal",
                   "atm_amt": "Amount via ATM Withdrawal",
                   "n_card_based": "Number of Debit Card Withdrawal",
                   "card_based_amt": "Amount via Debit Card Withdrawal",
                   "n_accounts_with_spend": "Number of T-Mobile Savings Accounts with Withdrawals",
                   "n_open_accounts": "Number of Open T-Mobile Savings Accounts",
                   "n_accounts_with_deposit": "Number of T-Mobile Savings Accounts with Deposit Transaction",
                   "n_deposits": "Number of Deposits Made to T-Mobile Savings Accounts",
                   "deposit_amt": "Amount via Deposits Made to T-Mobile Savings Accounts",
                   "n_deposit_ach": "Number of ACH Deposits",
                   "deposit_ach_amt": "Amount via ACH Deposits",
                   "n_rdc": "Number of RDC Deposits",
                   "rdc_amt": "Amount via RDC Deposits",
                   "n_wire": "Number of Wire Deposits",
                   "wire_amt": "Amount via Wire Deposits",
                   "n_mailed_in_check": "Number of Check - (Mailed in) Deposits",
                   "mailed_in_check_amt": "Amount via Check - (Mailed in) Deposits",
                   "total_ledger_balance_amt": "Ledger Balance in T-Mobile Savings Accounts",
                   "available_balance_amt": "Available Balance in T-Mobile Savings Accounts"
                  }, inplace=True)
    
    calculate_avgs('spend',df)
    calculate_avgs('deposit',df)
    calculate_avgs('balance',df)

    # reorder the columns
    df = df.reindex(columns=[
        'Wireless Flag',
        'Primary Account Holder Flag',
        'Postpaid Flag', 
        'Eligible Flag',
        'Account Type Code',
        'Post Date', 
        'Number of Withdrawals Made from T-Mobile Savings Accounts', 
        'Amount via Withdrawal Made from T-Mobile Savings Accounts', 
        'Average Withdrawal Amount from T-Mobile Savings Account',
        'Number of ACH Withdrawal', 
        'Amount via ACH Withdrawal', 
        'Average Withdrawal Amount through ACH',
        'Number of Paper Check Withdrawal', 
        'Amount via Paper Check Withdrawal', 
        'Average Withdrawal Amount through Paper Check',
        'Number of Bill Pay Withdrawal',
        'Amount via Bill Pay Withdrawal',
        'Average Withdrawal Amount through Bill Pay',
        'Number of ATM Withdrawal', 
        'Amount via ATM Withdrawal', 
        'Average Withdrawal Amount through ATM',
        'Number of Debit Card Withdrawal', 
        'Amount via Debit Card Withdrawal',
        'Average Withdrawal Amount through Debit Card',
        'Number of Open T-Mobile Savings Accounts',
        'Number of T-Mobile Savings Accounts with Withdrawals',
        'Percentage of Accounts Making a Withdrawal',
        'Number of Deposits Made to T-Mobile Savings Accounts',
        'Amount via Deposits Made to T-Mobile Savings Accounts',
        'Average Deposit Amount to T-Mobile Savings Account',
        'Number of ACH Deposits',
        'Amount via ACH Deposits',
        'Average Amount of Deposit by ACH',
        'Number of RDC Deposits',
        'Amount via RDC Deposits',
        'Average Amount of Deposit by RDC',
        'Number of Wire Deposits',
        'Amount via Wire Deposits',
        'Average Amount of Deposit by Wire',
        'Number of Check - (Mailed in) Deposits',
        'Amount via Check - (Mailed in) Deposits',
        'Average Amount of Deposit by Check - (Mailed in)',
        'Number of T-Mobile Savings Accounts with Deposit Transaction',
        'Percentage of Accounts Making a Deposit'])     

def shapeAccountOpeningData(df):
    # rename columns per requirements
    df.rename(columns={"IS_WIRELESS_FLAG": "Wireless Flag", 
                   "IS_PRIMARY_ACCOUNT_HOLDER_FLAG": "Primary Account Holder Flag",
                   "IS_POSTPAID_FLAG": "Postpaid Flag",
                   "IS_ELIGIBLE_FLAG": "Eligible Flag",
                   "ACCOUNT_TYPE_CODE": "Account Type Code",
                   "POST_DATE": "Post Date",
                   "n_accounts_with_deposit": "Number of T-Mobile Savings Accounts with Opening Deposit",
                   "deposit_amt": "Opening Deposit T-Mobile Savings Account",
                   "n_accounts_with_ach_deposit": "Number of Accounts with ACH Opening Deposit",
                   "deposit_ach_amt": "Opening ACH Deposit Amount",
                   "n_accounts_with_rdc_deposit": "Number of Accounts with RDC Opening Deposit",
                   "rdc_amt": "Opening RDC Deposit Amount",
                   "n_accounts_with_wire_deposit": "Number of Accounts with Wire Opening Deposit",
                   "wire_amt": "Opening Wire Deposit Amount",
                   "n_accounts_with_mailed_in_check_deposit": "Number of Accounts with Check Opening Deposit",
                   "mailed_in_check_amt": "Amount Deposit Made by Check",
                   "n_accounts_with_dda_transfer_deposit": "Number of Accounts with DDA Transfer Deposit",
                   "dda_transfer_amt": "Amount of Deposits Made by DDA Transfers"
                  }, inplace=True)
    
    calculateAccountOpeningAvgs(df)

    # reorder the columns
    df = df.reindex(columns=[
        'Wireless Flag',
        'Primary Account Holder Flag',
        'Postpaid Flag', 
        'Eligible Flag',
        'Account Type Code',
        'Post Date',
        'Number of T-Mobile Savings Accounts with Opening Deposit',
        'Opening Deposit T-Mobile Savings Account',
        'Average Opening Deposit Amount to T-Mobile Savings Accounts',
        'Number of Accounts with ACH Opening Deposit',
        'Opening ACH Deposit Amount',
        'Average Deposit Made by ACH',
        'Number of Accounts with RDC Opening Deposit',
        'Opening RDC Deposit Amount',
        'Average Deposit Made by RDC',
        'Number of Accounts with Wire Opening Deposit',
        'Opening Wire Deposit Amount',
        'Average Deposit Made by Wire',
        'Number of Accounts with Check Opening Deposit',
        'Amount Deposit Made by Check',
        'Average Deposit Made by Check',
        'Number of Accounts with DDA Transfer Deposit',
        'Amount of Deposits Made by DDA Transfers',
        'Average Deposit Made by DDA Transfer'])           

# function to averages
def calculateAccountOpeningAvgs(df):
    df['Average Opening Deposit Amount to T-Mobile Savings Accounts'] = df['Opening Deposit T-Mobile Savings Account']/df['Number of T-Mobile Savings Accounts with Opening Deposit']
    df['Average Deposit Made by ACH'] = df['Opening ACH Deposit Amount']/df['Number of Accounts with ACH Opening Deposit']
    df['Average Deposit Made by RDC'] = df['Opening RDC Deposit Amount']/df['Number of Accounts with RDC Opening Deposit']
    df['Average Deposit Made by Check'] = df['Amount Deposit Made by Check']/df['Number of Accounts with Check Opening Deposit']
    df['Average Deposit Made by Wire'] = df['Opening Wire Deposit Amount']/df['Number of Accounts with Wire Opening Deposit']        
    df['Average Deposit Made by DDA Transfer'] = df['Amount of Deposits Made by DDA Transfers']/df['Number of Accounts with DDA Transfer Deposit']        


 