from pyrogram import Client, filters
from pyrogram.types import ReplyKeyboardMarkup, InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.enums import ParseMode

from data import *

from datetime import datetime
from traceback import print_exc
from random import randrange as rd

import mysql.connector
import threading

app = Client("Teste",
            api_id=api_id,
            api_hash=api_hash,
            bot_token=bot_token)


with app:
    pass

lock = threading.Lock()

################## MySQL #################
def bd():
    global con, cur1, cur2, cur3

    con = mysql.connector.connect(
        host=dbhost,
        user=dbuser,
        password=dbpasswd,
        database=dbname
    )

    cur = con.cursor(buffered=True) # cursor para criar tabelas
    cur1 = con.cursor(buffered=True) # cursor para tabela clientes
    cur2 = con.cursor(buffered=True) # cursor para tabela sorteios
    cur3 = con.cursor(buffered=True) # cursor para a tabela codigos

    cur.execute(
        "create table if not exists clientes ("
        "id int auto_increment primary key,"
        "cod bigint not null,"
        "nome varchar(30) not null,"
        "unique(cod))"
    )


    cur.execute(
        "create table if not exists sorteios ("
        "id int auto_increment primary key,"
        "nome varchar(30) not null,"
        "max_ptc bigint not null,"
        "unique(nome))"
    )

    cur.execute(
        "create table if not exists cupons ("
        "id int auto_increment primary key,"
        "nome varchar(30) not null,"
        "user_cod bigint not null,"
        "sorteio varchar(30) not null,"
        "cupom bigint not null,"
        "constraint fk_nome foreign key (nome) references clientes(nome),"
        "constraint fk_user foreign key (user_cod) references clientes(cod),"
        "constraint fk_sorteio foreign key (sorteio) references sorteios(nome))"
    )
    

############## COMANDOS ####################

@app.on_message(filters.private & filters.command("start"))
def start(bot, mensagem):
    m_id = mensagem.id
    user_id = mensagem.chat.id
    fname = str(mensagem.chat.first_name)

    if m_id == 1:
        app.send_message(user_id, "Mensagem que inicio para primeira interação")
    else:
        app.send_message(user_id, "Mensagem de inicio")
    registrar(user_id, fname)

@app.on_message(filters.private & filters.command("rsorteio"))
def rSorteio(bot, mensagem):
    user_id = mensagem.chat.id
    txt = mensagem.text.split()

    if len(txt) < 3:
        app.send_message(user_id, "Para registrar novo sorteio, envie:\n\n/rsorteio <nome> <participantes-max>", parse_mode=ParseMode.MARKDOWN)
    else:
        sort_name = txt[1]
        sort_ptc = txt[2]
        r = bdMap(2, "insert into sorteios(nome, max_ptc) values(%s, %s)", [sort_name, sort_ptc], "insert")
        if r == "duplicate":
            app.send_message(user_id, f"O sorteio {sort_name} já existe!")
        else:
            app.send_message(user_id, f"O sorteio {sort_name} foi registrado!")
            

@app.on_message(filters.private & filters.command("sorteios"))
def sorteios(bot, mensagem):
    sorts = bdMap(2, "select * from sorteios")
    user_id = mensagem.chat.id
    btns = []

    if len(sorts) > 0:
        for sort in sorts:
            btns.append([InlineKeyboardButton(sort[1], callback_data=f"sort_{sort[1]}")])
        
        markup = InlineKeyboardMarkup(btns)

        app.send_message(user_id, "Esses são os sorteios disponiveis.\n\nPara retirar um cupom, clique no sorteio que deseja participar!", reply_markup=markup)
    else:
        app.send_message(user_id, "Desculpe, não existe nenhum sorteio ativo no momento")


############# UTILS #############

def registrar(user_id, fname):
    try:
        r = bdMap(1, "insert into clientes(cod, nome) values(%s, %s)", [user_id, fname], "insert")
        if r == "duplicate":
            app.send_message(user_id, "Usuario já cadastrado!")
        else:
            app.send_message(user_id, "Usuario cadastrado!")
    except Exception as errorrg:
        print(errorrg)

def cupom(nome, user_id, sorteio):
    cupons = [x[4] for x in bdMap(3, "select * from cupons where sorteio=%s", [sorteio])]
    num = rd(1, 10000)

    while num in cupons:
        num = rd(1, 10000)

    bdMap(3, "insert into cupons(nome, user_cod, sorteio, cupom) values(%s, %s, %s, %s)", [nome, user_id, sorteio, num], "insert")

    app.send_message(user_id, f"Seu cupom é {num} para o sorteio {sorteio}")


def bdMap(c, sql, var=None,  method="select"): #Interações com banco de dados
    cursors = {
        1: cur3,
        2: cur2,
        3: cur3
    }

    lock.acquire(True)
    log(f"Executando {c}, {sql}, {var}, {method}\n")
    try:
        if method == "select":
            if var is None:
                cursors[c].execute(sql)
                item = cursors[c].fetchall()
            else:
                cursors[c].execute(sql, var)
                item = cursors[c].fetchall()
            return item
        else:
            if var is None:
                cursors[c].execute(sql)
            else:
                cursors[c].execute(sql, var)
            con.commit()
    except Exception as e:
        log(f"Erro: {e}\n")
        con.rollback()
        if "Duplicate entry" in str(e):
            return "duplicate"
    finally:
        log(f"Executado {c}, {sql}, {var}, {method}\n\n")
        lock.release()

def log(texto):
    with open("log.txt", "a+", encoding="utf-8") as arq:
        arq.write(f"[{datetime.now().strftime('%x %X.%f')}] ")
        arq.write(texto)

############### CALLBACKS ##############

@app.on_callback_query(filters.regex("^sort\S"))
def callSort(bot, call):
    user_id = call.from_user.id
    nome = call.from_user.first_name
    sorteio = str(call.data)[5:]
    cupom(nome, user_id, sorteio)

if __name__ == "__main__":
    bd()
    app.run()
