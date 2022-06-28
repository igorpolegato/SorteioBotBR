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

@app.on_message(filters.private & filters.command("help"))
def helpC(bot, mensagem):
    user_id = mensagem.chat.id

    btns = [
        [InlineKeyboardButton("Sorteios", callback_data="help_sorteios"),
        InlineKeyboardButton("Cupons", callback_data="help_cupons")]
    ]

    markup = InlineKeyboardMarkup(btns)

    app.send_message(user_id, "Esses são meus comandos, clique neles para usa-los!\n\nPara pegar um cupom, veja os sorteios disponiveis", reply_markup=markup)

@app.on_message(filters.private & filters.command("rsorteio"))
def rSorteio(bot, mensagem):
    user_id = mensagem.chat.id
    txt = mensagem.text.split()

    if len(txt) < 2:
        app.send_message(user_id, "Para registrar novo sorteio, envie:\n\n/rsorteio <nome>", parse_mode=ParseMode.MARKDOWN)
    else:
        sort_name = txt[1]
        r = bdMap(2, "insert into sorteios(nome) values(%s)", [sort_name], "insert")
        if r == "duplicate":
            app.send_message(user_id, f"O sorteio {sort_name} já existe!")
        else:
            app.send_message(user_id, f"O sorteio {sort_name} foi registrado!")

@app.on_message(filters.private & filters.command("rmsorteio"))
def rmSorteio(bot, mensagem):
    user_id = mensagem.chat.id
    txt = mensagem.text.split()

    if len(txt) != 2 or txt[0] != "/rmsorteio":
        app.send_message(user_id, "Para apagar um sorteio, envie:\n\n/rmsorteio <nome>", parse_mode=ParseMode.MARKDOWN)
    
    else:
        sort_name = txt[1]
        bdMap(3, "delete from cupons where sorteio=%s", [sort_name], "delete")
        bdMap(2, "delete from sorteios where nome=%s", [sort_name], "delete")

        app.send_message(user_id, f"O sorteio {sort_name} foi removido!")



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

@app.on_message(filters.private & filters.command("cupons"))
def consultarCp(bot, mensagem):
    user_id = mensagem.chat.id
    todos = bdMap(3, "select * from cupons where user_cod=%s", [user_id])
    msg = "Esses são os sorteios que você possui cupons: \n\n"
    cps = {}

    for dado in todos:
        sorteio = dado[3]
        cp = dado[4]

        if sorteio not in cps.keys():
            cps[sorteio] = []
        
        if cp not in cps[sorteio]:
            cps[sorteio].append(cp)

    for k, v in cps.items():
        msg += f"**{k}**\n"

        for c in v:
            msg += f"  - {c}\n"
        
        msg += "\n"

    app.send_message(user_id, msg)


@app.on_message(filters.private & filters.command("enviar"))
def enviar(bot, mensagem):
    user_id = mensagem.chat.id
    media = str(mensagem.media).replace("MessageMediaType.", "").lower()
    users = [u[1] for u in bdMap(1, "select * from clientes")]

    met = {
        "text": app.send_message,
        "video": app.send_video,
        "photo": app.send_photo
    }

    if media == "none":
        text = mensagem.text.replace("/enviar", "")

        for user in users:
            met['text'](user, text)

    else:
        text = mensagem.caption.replace("/enviar ", "")

        types = {
            "video": mensagem.video,
            "photo": mensagem.photo
        }

        for user in users:
            met[media](user, types[media].file_id, text)


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
        1: cur1,
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

@app.on_callback_query(filters.regex("^help_sorteios"))
def callSorteios(bot, call):
    sorteios(bot, call.message)

@app.on_callback_query(filters.regex("^help_cupons"))
def callCupons(bot, call):
    consultarCp(bot, call.message)

if __name__ == "__main__":
    bd()
    app.run()
