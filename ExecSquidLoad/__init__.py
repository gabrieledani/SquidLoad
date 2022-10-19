import datetime
import configparser
import logging

import requests
import json
import psycopg2
import re

import azure.functions as func

def Transacoes(host, user, dbname, password, sslmode): #Atualizado em 11/03

  # string De Conexão
  conn_string = "host={0} user={1} dbname={2} password={3} sslmode={4}".format(host, user, dbname, password, sslmode)

  # estabelecendo Conexão
  conn = psycopg2.connect(conn_string)
  cursor = conn.cursor()

  data_ontem = datetime.datetime.today() - datetime.timedelta(days=2)
  data_ontem = data_ontem.strftime("%Y-%m-%d")    #'2022-02-25' #data_ontem.strftime("%Y-%m-%d")  #'2022-01-07' # 
  data_hoje = datetime.datetime.today()
  data_hoje  =  data_hoje.strftime("%Y-%m-%d")



  # URL DA API
  url = "https://api.veripag.com.br/split/v1/sale?start_date=" + str(data_ontem) +"&end_date=" + str(data_hoje)

  # CHAVES API
  payload={}
  headers = {
    'api_key': 'ak_live_82b9ef239c250fdbcd86274e9ef0043e6b8cf7e2',
    'Authorization': 'Bearer 58c9052e-ef69-48e0-a1c4-616a7a2a8fc3'
  }

  # Retorno API via GET
  response = requests.request("GET", url, headers=headers, data=payload)
  retornoAPI = response.json()
  #print(retornoAPI)


  Sql =  "delete from mep_transacoes where data_transacao >= current_date-2" 
  cursor.execute (Sql)
  conn.commit()


  # ANALISANDO RETORNO API
  #x = retornoAPI['content']
  #print(x)

  pagina =retornoAPI['page']
  pagina_total = retornoAPI['total_pages']

  for x in range(1,pagina_total+1):
    
    url = "https://api.veripag.com.br/split/v1/sale?start_date=" + str(data_ontem) +"&end_date=" + str(data_hoje) + "&page=" + str(x)
    #print(url)
    # Retorno API via GET
    response = requests.request("GET", url, headers=headers, data=payload)
    retornoAPI = response.json()
    #print(retornoAPI)

    for i in retornoAPI['content']:
    #inserindo dados   
      #id_status = str(i['installments'][0]['sale_status']['id'])
      desc_status = str(i['installments'][0]['sale_status']['name'])
      antecipado = str(i['installments'][0]['amount']['antecipated_amount'])
      acquirer_amount = str(i['installments'][0]['amount']['acquirer_amount'])
      data_pagamento = '2021-01-01' #i['installments']['payment_date']
      Sql = "INSERT INTO mep_transacoes(transacao_id, data_transacao, cliente_id, terminal_id, bandeira, adquirente, cod_mcc, data_pay, nsu_terminal, mdr_adquirente, mdr_squid, pagina, qtde, spread, status, tipo_venda, vlr_bruto, vlr_liquido,antecipado,vlr_liq_antecipacao) VALUES ('" + str(i['id']) + "','" + str(i['transaction_date']) + "','" + str(i['merchant']['merchant_id']) + "','" + str(i['terminal']['id']) + "','" + i['brand']['name'] + "','" + i['acquirer']['name'] + "','" + i['Mcc']['code'] + "','" +    data_pagamento + "','" + str(i['terminal_nsu']) + "','" + str(i['amount']['acquirer_amount'] ) + "','"  + str(i['amount']['fee']) + "','" + str(pagina) + "','" + str(i['installment_qtd']) + "','" + str(i['amount']['spread'])    + "','" + desc_status + "','" + i['sale_type']['name']  +"','" + str(i['amount']['value'])   + "','" + str(i['amount']['net_Amount']) + "','" + antecipado +"','" + acquirer_amount + "')"
      print(Sql)
      cursor.execute (Sql)
      #print(str(i['installments']['payment_date']))
      conn.commit() 


  cursor.close()
  conn.close()

def Clientes(host, user, dbname, password, sslmode): #Alterado em 07/03

    # string De Conexão
    conn_string = "host={0} user={1} dbname={2} password={3} sslmode={4}".format(host, user, dbname, password, sslmode)

    # estabelecendo Conexão
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()


    

    # URL DA API
    url = "https://api.veripag.com.br/split/v1/merchant"

    # CHAVES API
    payload={}
    headers = {
    'api_key': 'ak_live_82b9ef239c250fdbcd86274e9ef0043e6b8cf7e2',
    'Authorization': 'Bearer 58c9052e-ef69-48e0-a1c4-616a7a2a8fc3'
    }

    # Retorno API via GET
    response = requests.request("GET", url, headers=headers, data=payload)
    retornoAPI = response.json()
    #print(retornoAPI)


    pagina =retornoAPI['page']
    pagina_total = retornoAPI['total_pages']

    Sql =  "truncate table  mep_clientes" 
    cursor.execute (Sql)
    conn.commit()





    for x in range(1,pagina_total+1):
        url = "https://api.veripag.com.br/split/v1/merchant?page=" + str(x)
        response = requests.request("GET", url, headers=headers, data=payload)
        retornoAPI = response.json()

        # ANALISANDO RETORNO API
        #x = retornoAPI['content']
        
        for i in retornoAPI['content']:
            #print(url)
            #inserindo dados   
            cliente_nomex = re.sub(r"[']","",i['name'])
            if i['representative'] == None:  
                rep_id = '0'
                rep_nome = 'Sem Rep'
            else:
                rep_id = str(i['representative']['representative_id']) 
                rep_nome = i['representative']['name']

        
            Sql =  "INSERT INTO mep_clientes(cliente_id, cliente, fantasia, representante_empresa, representante_id, contato, categoria, antecipacao, ativo, numero, pagamento_automatico, tp_cliente_pj_pf, cidade, estado, pais, dt_cadastro,representante_nome) VALUES ('" + i['id'] + "', '" + cliente_nomex + "','" + i['soft_descriptor'] + "', '" + i['contact_name'] + "'," + rep_id + ",'" + i['contact_name'] + "', '" + i['category'] + "',  '" + str(i['show_antecipation']) + "',  '" + str(i['is_Active']) + "',  '" + str(i['number']) + "',  '" + str(i['automatic_Payment']) + "','" + i['type_Company'] + "', '" + i['city'] + "', '" + i['state'] + "', '" + i['country']   + "', '" + i['registerDate'] + "','" + rep_nome + "')"
            #i[representative][representative_id]
            print(Sql)
            cursor.execute (Sql)
            #print(i['registerDate'])
            conn.commit() 



    #apagando todos os agentes
    SqlA =  "delete from  mep_agentes" 
    cursor.execute (SqlA)
    conn.commit()

    #recriando todos os agentes atraves dos clientes
    SqlAI = "insert into mep_agentes (agente,representante_id,client_id) select distinct representante_nome , to_number(representante_id,'999'),to_number(cliente_id,'999999') from mep_clientes"
    cursor.execute (SqlAI)
    conn.commit()


    #update na tabela de transacoes colocando os agentes dos ultimos 3 dias
    SqlB ="UPDATE mep_transacoes SET id_agente = mep_agentes.representante_id, nome_agente = mep_agentes.agente FROM mep_agentes WHERE mep_transacoes.cliente_id = mep_agentes.client_id  and data_transacao >= current_date -3"
    cursor.execute (SqlB)
    conn.commit()


    cursor.close()
    conn.close()

def Bancos(host, user, dbname, password, sslmode): 
  # string De Conexão
  conn_string = "host={0} user={1} dbname={2} password={3} sslmode={4}".format(host, user, dbname, password, sslmode)

  # estabelecendo Conexão
  conn = psycopg2.connect(conn_string)
  cursor = conn.cursor()

  # URL DA API
  url = "https://api.veripag.com.br/split/v1/establishment/banks"

  # CHAVES API
  payload={}
  headers = {
    'api_key': 'ak_live_82b9ef239c250fdbcd86274e9ef0043e6b8cf7e2',
    'Authorization': 'Bearer 58c9052e-ef69-48e0-a1c4-616a7a2a8fc3'
  }

  # Retorno API via GET
  response = requests.request("GET", url, headers=headers, data=payload)
  retornoAPI = response.json()
  #print(retornoAPI)

  Sql =  "delete from  mep_bancos" 
  cursor.execute (Sql)
  conn.commit()

  # ANALISANDO RETORNO API
  #x = retornoAPI['content']
  #print(retornoAPI['page'])
  for i in retornoAPI:
      #inserindo dados   
      re.sub(r"[']","",i['name'])
      Sql =  "INSERT INTO mep_bancos(banco_id, banco) VALUES ( '" + i['bank_id'] + "','" + re.sub(r"[']","",i['name']) +"')" 
      #print(Sql)
      cursor.execute (Sql)
      #print(i['bank_id'])
      conn.commit()
  cursor.close()
  conn.close()

def Terminais(host, user, dbname, password, sslmode): 

  # string De Conexão
  conn_string = "host={0} user={1} dbname={2} password={3} sslmode={4}".format(host, user, dbname, password, sslmode)

  # estabelecendo Conexão
  conn = psycopg2.connect(conn_string)
  cursor = conn.cursor()

  # URL DA API
  url = "https://api.veripag.com.br/split/v1/terminal"

  # CHAVES API
  payload={}
  headers = {
    'api_key': 'ak_live_82b9ef239c250fdbcd86274e9ef0043e6b8cf7e2',
    'Authorization': 'Bearer 58c9052e-ef69-48e0-a1c4-616a7a2a8fc3'
  }

  # Retorno API via GET
  response = requests.request("GET", url, headers=headers, data=payload)
  retornoAPI = response.json()
  #print(retornoAPI)

  Sql =  "truncate table  mep_terminais" 
  cursor.execute (Sql)
  conn.commit()

  pagina =retornoAPI['page']
  pagina_total = retornoAPI['total_pages']

  # ANALISANDO RETORNO API

  for x in range(1,pagina_total+1):
      url = "https://api.veripag.com.br/split/v1/terminal?page=" + str(x)
      response = requests.request("GET", url, headers=headers, data=payload)
      retornoAPI = response.json()
          
      for i in retornoAPI['content']:
          #inserindo dados   
          #re.sub(r"[']","",i['name'])
          Sql =  "INSERT INTO mep_terminais(	terminal_id, serial_number, marca, modelo, plataforma, ativo)	VALUES ( '" + str(i['id']) + "','" + i['serial_number'] + "','" + i['manufacturer']  + "','" + i['model'] + "','" + i['plataform_type'] + "','" + str(i['active']) +"')"
          #print(Sql)
          cursor.execute (Sql)
          #print(i['bank_id'])
          conn.commit()

  cursor.close()
  conn.close()

def Mcc(host, user, dbname, password, sslmode): 

  # string De Conexão
  conn_string = "host={0} user={1} dbname={2} password={3} sslmode={4}".format(host, user, dbname, password, sslmode)

  # estabelecendo Conexão
  conn = psycopg2.connect(conn_string)
  cursor = conn.cursor()


  # URL DA API
  url = "https://api.veripag.com.br/split/v1/establishment/mcc"

  # CHAVES API
  payload={}
  headers = {
    'api_key': 'ak_live_82b9ef239c250fdbcd86274e9ef0043e6b8cf7e2',
    'Authorization': 'Bearer 58c9052e-ef69-48e0-a1c4-616a7a2a8fc3'
  }

  # Retorno API via GET
  response = requests.request("GET", url, headers=headers, data=payload)
  retornoAPI = response.json()
  #print(retornoAPI)

  Sql =  "delete from  mep_mcc" 
  cursor.execute (Sql)
  conn.commit()

  # ANALISANDO RETORNO API
  #x = retornoAPI['content']
  #print(retornoAPI['page'])
  for i in retornoAPI:
      #inserindo dados   
      name_mcc =re.sub(r"[']","",i['name'])
      #Sql =  "INSERT INTO mep_bancos(banco_id, banco) VALUES ( '" + i['bank_id'] + "','" + re.sub(r"[']","",i['name']) +"')" 
      Sql =  "INSERT INTO public.mep_mcc(desc_mcc, cod_mcc, mcc_id) 	VALUES ( '" + name_mcc + "','" + str(i['code']) +  "','" + str(i['id'])  +"')" 
      #print(Sql)
      cursor.execute (Sql)
      #print(Sql)
      conn.commit()
  cursor.close()
  conn.close()

def main(mytimer: func.TimerRequest) -> None:
    #utc_timestamp = datetime.datetime.now().replace(tzinfo=datetime.timezone.utc).isoformat()
    utc_timestamp = datetime.datetime.now().isoformat()
    logging.info('Python timer trigger function ran 2 at %s', utc_timestamp)

    # iNFORMAÇÕES DE CONEXAO
    host = "dwsquid.postgres.database.azure.com"
    dbname = "dwSquid"
    user = "squid"
    password = "S@Q_*idCxs#X#!DB@Dig@l"
    sslmode = "require"

    # neste caso foi chamada a função Transacoes do arquivo que importa transacoes do banco
    
    print("executanto transações...")
    #logging.info('executanto transações... at %s', utc_timestamp)

    Transacoes(host, user, dbname, password, sslmode)
    
    print("executanto Clientes...")
    #logging.info('executanto Clientes... at %s', utc_timestamp)

    Clientes(host, user, dbname, password, sslmode)
    
    print("executanto bancos...")
    #logging.info('executanto bancos... at %s', utc_timestamp)

    Bancos(host, user, dbname, password, sslmode)
    
    print("executanto Terminais")
    #logging.info('executanto Terminais... at %s', utc_timestamp)

    Terminais(host, user, dbname, password, sslmode)
    
    print("executanto MCC..")
    #logging.info('executanto MCC... at %s', utc_timestamp)

    Mcc(host, user, dbname, password, sslmode)

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
