from simple_salesforce import Salesforce
import requests
import psycopg2
import psycopg2.extras
import psycopg2
import psycopg2.extras
import connection

try:

    session = requests.Session()
    # manipulate the session instance (optional)
    sf = Salesforce(
        username=connection.sfusername(),
        password=connection.sfpassword(),
        security_token=connection.sfsecuritytoken(),
        session=session)

    query = '''SELECT CampaignId,
                       Id,
                       ContactId,
                       CreatedById,
                       CreatedDate,
                       IsDeleted,
                       FirstRespondedDate,
                       LastModifiedById,
                       LastModifiedDate,
                       LeadId,
                       HasResponded,
                       Status,
                       SystemModstamp,
                       Account_Record_Type__c,
                       Actual_Responded_Date__c,
                       City,
                       Country,
                       Lead_Contact_Status__c,
                       Account_Owner__c 
                from CampaignMember 
                LIMIT 50000'''
    records = sf.bulk.CampaignMember.query(query)
    conn = psycopg2.connect(**connection.connect())
    cursor = conn.cursor()
    cursor.execute("drop table if exists campaign_member")
    cursor.execute('''create temp table campaign_member(
                                                            campaignId varchar,
                                                            id varchar, 
                                                            contactId varchar,
                                                            createdBy varchar, 
                                                            createdDate bigint,
                                                            isDeleted boolean,
                                                            firstRespondedDate date,
                                                            lastModifiedBy varchar, 
                                                            lastModifiedDate bigint, 
                                                            leadId varchar,
                                                            hasResponded boolean,
                                                            status varchar,
                                                            systemModstamp bigint, 
                                                            account_record_type varchar, 
                                                            actual_responded_date date, 
                                                            city varchar, 
                                                            country varchar, 
                                                            leadcontactstatus varchar, 
                                                            accountowner varchar)''')
    for record in records:
        strr = '''insert into campaign_member (campaignId,
                                               id, 
                                               contactId,
                                               createdBy,
                                               createdDate,
                                               isDeleted,
                                               firstRespondedDate,
                                               lastModifiedBy, 
                                               lastModifiedDate, 
                                               leadId,
                                               hasResponded,
                                               status,
                                               systemModstamp, 
                                               account_record_type, 
                                               actual_responded_date, 
                                               city, 
                                               country, 
                                               leadcontactstatus, 
                                               accountowner) 
                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        parameters = record['CampaignId'],\
            record['Id'],\
            record['ContactId'],\
            record['CreatedById'],\
            record['CreatedDate'],\
            record['IsDeleted'],\
            record['FirstRespondedDate'],\
            record['LastModifiedById'],\
            record['LastModifiedDate'],\
            record['LeadId'],\
            record['HasResponded'],\
            record['Status'],\
            record['SystemModstamp'],\
            record['Account_Record_Type__c'],\
            record['Actual_Responded_Date__c'],\
            record['City'],\
            record['Country'],\
            record['Lead_Contact_Status__c'],\
            record['Account_Owner__c']
        cursor.execute(strr, parameters)
    conn.commit()
    cnt = cursor.rowcount
    if (cnt > 0):
        cursor.execute("drop table if exists corp_dw.sf_campaign_member")
        cursor.execute('''create table corp_dw.sf_campaign_member as 
                                select campaignId, 
                                        c.id,
                                        c.contactId,
                                        u.name createdBy, 
                                        to_timestamp(c.createdDate/1000) createdDate,
                                        isDeleted,
                                        firstRespondedDate,
                                        u1.name lastModifiedBy, 
                                        to_timestamp(lastModifiedDate/1000) lastModifiedDate, 
                                        leadId,
                                        hasResponded,
                                        status,
                                        to_timestamp(systemModstamp/1000) systemModstamp,
                                        account_record_type,
                                        actual_responded_date,
                                        city,
                                        country,
                                        leadcontactstatus,
                                        accountowner 
                                from campaign_member c 
                                left join corp_dw.sf_users u on u.id=c.createdBy 
                                left join corp_dw.sf_users u1 on u1.id=c.lastModifiedBy''')
        cursor.execute(
            "alter table corp_dw.sf_campaign_member owner to dw_user")
        cursor.execute(
            "grant select on corp_dw.sf_campaign_member to salesforce_role")

    conn.commit()
    print("campaign members sync done.... " + str(cnt))
except psycopg2.Error as e:
    print(e)
    print("error in campaign member sync occurred")
conn.close()
