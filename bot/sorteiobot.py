from pyrogram import Client, filters
from pyrogram.types import ReplyKeyboardMarkup, InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.enums import ParseMode

from data import *

from datetime import datetime
from traceback import print_exc
from random import randrange as rd, choice

import mysql.connector
import threading

app = Client("GSorteio",
            api_id=api_id,
            api_hash=api_hash,
            bot_token=bot_token)


with app:
    pass

lock = threading.Lock()

add_regra = {}
add_sorteio = {}

################## MySQL #################
def bd():
    global con, cur1, cur2, cur3, cur4, cur5

    con = mysql.connector.connect(
        host=dbhost,
        user=dbuser,
        password=dbpasswd,
        database=dbname,
    )

    cur = con.cursor(buffered=True) # cursor para criar tabelas
    cur1 = con.cursor(buffered=True) # cursor para tabela clientes
    cur2 = con.cursor(buffered=True) # cursor para tabela sorteios
    cur3 = con.cursor(buffered=True) # cursor para a tabela cupons
    cur4 = con.cursor(buffered=True) # cursor para a tabela indicados
    cur5 = con.cursor(buffered=True) # cursor para a tabela regras

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
        "criador bigint not null,"
        "ganhadores int not null default 1)"
    )

    cur.execute(
        "create table if not exists cupons ("
        "id int auto_increment primary key,"
        "nome varchar(30) not null,"
        "user_cod bigint not null,"
        "sorteio int not null,"
        "cupom bigint not null,"
        "constraint fk_nome foreign key (nome) references clientes(nome),"
        "constraint fk_user foreign key (user_cod) references clientes(cod),"
        "constraint fk_sorteio foreign key (sorteio) references sorteios(id))"
    )
    
    cur.execute(
        "create table if not exists indicados ("
        "id int auto_increment primary key,"
        "indicante bigint not null,"
        "n_indicado varchar(30) not null,"
        "indicado bigint not null,"
        "constraint fk_user_ind foreign key (indicante) references clientes(cod),"
        "unique(indicado))"
    )

    cur.execute(
        "create table if not exists regras ("
        "id int auto_increment primary key,"
        "sorteio int not null,"
        "regras varchar(200) not null,"
        "constraint fk_sort_regras foreign key (sorteio) references sorteios(id))"
    )

############## COMANDOS ####################

@app.on_message(filters.private & filters.command("start")) #Resposta para o comando start, que ?? enviado quando um usu??rio inicia o bot
def start(bot, mensagem):
    user_id = mensagem.chat.id
    fname = str(mensagem.chat.first_name)

    registrar(user_id, fname)
    helpC(bot, mensagem)
    sorteios(bot, mensagem)

@app.on_message(filters.private & filters.command("help")) #Resposta para o comando help, que consulta a maioria dos comando dispon??veis do bot
def helpC(bot, mensagem):
    user_id = mensagem.chat.id
    fname = mensagem.chat.first_name

    btns = [
        [InlineKeyboardButton("Sorteios", callback_data="help_sorteios"), InlineKeyboardButton("Cupons", callback_data="help_cupons")],
        [InlineKeyboardButton("Registrar Sorteio", callback_data="help_regsorteio"), InlineKeyboardButton("Apagar sorteio", callback_data="help_rmsorteio")],
        [InlineKeyboardButton("Sortear", callback_data="help_sortear"), InlineKeyboardButton("Atualizar regras", callback_data="help_atregras")],
        [InlineKeyboardButton("Indica????o", callback_data="help_ind")]

    ]

    markup = InlineKeyboardMarkup(btns)

    app.send_message(user_id, "Esses s??o meus comandos, clique neles para usa-los!\n\nPara pegar um cupom, veja os sorteios disponiveis", reply_markup=markup)
    print(f"O usu??rio {fname}({user_id}) consultou os comandos --> /help\n")

@app.on_message(filters.private & filters.command("rsorteio")) #Respota para o comando rsorteio, que registra um novo sorteio
def rSorteio(bot, mensagem):
    user_id = mensagem.chat.id
    txt = mensagem.text.split()

    if len(txt) < 2 or txt[0] != "/rsorteio":
        app.send_message(user_id, "Para registrar novo sorteio, envie:\n\n/rsorteio <nome>", parse_mode=ParseMode.MARKDOWN)
    else:
        sort_name = " ".join(txt[1:]).lower()
        sort_name = sort_name.title()

        if sort_name not in add_sorteio.keys():
            add_sorteio[user_id] = sort_name
            app.send_message(user_id, "Envie a quantidade de ganhadores do sorteio")

@app.on_message(filters.private & filters.command("rmsorteio")) #Resposta para o comando rmsorteio, que deleta um sorteio da lista
def rmSorteio(bot, mensagem):
    user_id = mensagem.chat.id

    sorts = bdMap(2, "select * from sorteios where criador=%s", [user_id])

    if len(sorts) > 0:
        btns = [[InlineKeyboardButton(f"{int(s[0]) + 150} - {s[1]}", callback_data=f"rmsort_{int(s[0]) + 150}_{s[1]}")] for s in sorts]
        markup = InlineKeyboardMarkup(btns)

        app.send_message(user_id, "Esses s??o os sorteios que voc?? possui cadastrado.\n\nClique no sorteio que deseja excluir", reply_markup=markup)        

    else:
        app.send_message(user_id, "Voc?? n??o possui nenhum sorteio cadastrado!")

@app.on_message(filters.private & filters.command("sorteios")) #Resposta para o comando sorteios, que consulta todos os sorteios dispon??veis
def sorteios(bot, mensagem):
    sorts = bdMap(2, "select * from sorteios")
    user_id = mensagem.chat.id
    fname = mensagem.chat.first_name
    btns = []

    if len(sorts) > 0:
        for sort in sorts:
            btns.append([InlineKeyboardButton(f"{sort[0] + 150} - {sort[1]}", callback_data=f"sort_{int(sort[0]) + 150}_{sort[1]}")])
        
        markup = InlineKeyboardMarkup(btns)

        app.send_message(user_id, "Esses s??o os sorteios disponiveis.\n\nPara retirar um cupom, clique no sorteio que deseja participar!", reply_markup=markup)
    else:
        app.send_message(user_id, "Desculpe, n??o existe nenhum sorteio ativo no momento")

    print(f"O usu??rio {fname}({user_id}) consultou os sorteios --> /sorteios\n")

@app.on_message(filters.private & filters.command("cupons")) #Resposta para o comando cupons, que consulta todos os cupons de todos os sorteios que o usu??rio participa
def consultarCp(bot, mensagem):
    user_id = mensagem.chat.id
    fname = mensagem.chat.first_name
    todos = bdMap(3, "select * from cupons where user_cod=%s", [user_id])
    msg = "Esses s??o os sorteios que voc?? possui cupons: \n\n"
    cps = {}

    for dado in todos:
        sorteio = str(dado[3] + 150)
        sorteio += f" - {bdMap(2, 'select nome from sorteios where id=%s', [int(sorteio) - 150])[0][0]}"
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

    print(f"O usu??rio {fname}({user_id}) consultou os cupons --> /cupons\n")

@app.on_message(filters.private & filters.command("indica")) #Resposta para o comando indica, que registra uma indica????o e gera um novo cupom
def indica(bot, mensagem):
    user_id = mensagem.chat.id
    fname = mensagem.chat.first_name
    text = mensagem.text.split()

    try:
        indicante = int(text[1])
    except Exception:
        indicante = False

    if len(text) != 2 or text[0] != "/indica" or not indicante:
        app.send_message(user_id, "Para registrar um indicante, digite:\n\n /indica <c??digo>", parse_mode=ParseMode.MARKDOWN)

    elif indicante == user_id:
        app.send_message(user_id, "Voc?? n??o pode usar seu pr??prio c??digo!")

    else:
        try:
            nome = bdMap(1, "select * from clientes where cod=%s", [indicante])[0][2]
            cupom(nome, indicante, fname=fname, ind=True, indicado=user_id)

        except Exception:
            app.send_message(user_id, "C??digo invalido!")
        

@app.on_message(filters.command("sortear")) #Op????es para o sorteio
def escSortear(bot, mensagem):
    user_id = mensagem.chat.id
    sorts = bdMap(2, "select * from sorteios where criador=%s", [user_id])

    if len(sorts) > 0:
        st = [[InlineKeyboardButton(f"{int(s[0] + 150)} - {s[1]}", callback_data=f"win_{int(s[0]) + 150}_{s[1]}")] for s in sorts]

        markup = InlineKeyboardMarkup(st)

        app.send_message(user_id, "Escolha de que sorteio o ganhador ser?? definido", reply_markup=markup)
    
    else:
        app.send_message(user_id, "Voc?? n??o possui nenhum sorteio cadastrado!")


@app.on_message(filters.private & filters.command("regras"))
def regras(bot, mensagem):
    user_id = mensagem.chat.id
    try:
        btns = [[InlineKeyboardButton(f"{s[0] + 150} - {s[1]}", callback_data=f"regras_{s[0] + 150}_{s[1]}")] for s in bdMap(2, "select * from sorteios where criador=%s", [user_id])]
        
        markup = InlineKeyboardMarkup(btns)

        app.send_message(user_id, "Selecione o sorteio para alterar as regras", reply_markup=markup)
    
    except Exception:
        app.send_message(user_id, "Voc?? n??o possui nenhum sorteio registrado!")

@app.on_message(filters.private & filters.command("enviar")) #Resposta para o comando enviar, que envia a mensagem para todos os usu??rios cadastrados
def enviar(bot, mensagem):
    user_id = mensagem.chat.id

    mensagem = mensagem.reply_to_message

    if user_id == adm_id:
        media = str(mensagem.media).replace("MessageMediaType.", "").lower()
        users = [u[1] for u in bdMap(1, "select * from clientes")]
        users.remove(user_id)

        met = {
            "text": app.send_message,
            "video": app.send_video,
            "photo": app.send_photo,
            "document": app.send_document
        }

        if media == "none":
            text = mensagem.text.replace("/enviar", "")

            for user in users:
                met['text'](user, text)
                print(f"Mensagem encaminhada para {user}\n")

        else:
            types = {
                    "video": mensagem.video,
                    "photo": mensagem.photo,
                    "document": mensagem.document
            }

            if mensagem.caption is not None:
                text = mensagem.caption.replace("/enviar ", "")

                for user in users:
                    met[media](user, types[media].file_id, caption=text)
                    print(f"Mensagem encaminhada para {user}\n")
            else:
                for user in users:
                    met[media](user, types[media].file_id)
                    print(f"Mensagem encaminhada para {user}\n")



@app.on_message(filters.command("teste"))
def teste(bot, mensagem):
    pass

@app.on_message(filters.private)
def rRegras(bot, mensagem):
    user_id = mensagem.chat.id
    text = mensagem.text

    ##################### Adicionar regra #####################
    if str(user_id) in add_regra.keys():
        sorteio = add_regra[str(user_id)]

        r = bdMap(5, "insert into regras(sorteio, regras) values(%s, %s)", [sorteio[0], text], "insert")
        if r == "duplicate":
            bdMap(5, "delete from regras where sorteio=%s", [sorteio[0]])
            r()

        add_regra.pop(str(user_id), 404)

        app.send_message(user_id, f"Regras para o sorteio {sorteio[1]} alteradas!")


    ################# Adicionar sorteio ########################
    elif user_id in add_sorteio.keys():
        sorteio = add_sorteio[user_id]

        if len(text.split()) == 1:
            
            bdMap(2, "insert into sorteios(nome, criador, ganhadores) values(%s, %s, %s)", [sorteio, user_id, int(text)], "insert")
            app.send_message(user_id, f"O sorteio {sorteio} foi cadastrado, com {text} {'ganhador' if int(text) == 1 else 'ganhadores'}")

            add_sorteio.pop(user_id, 404)
        
        else:
            app.send_message(user_id, "N??mero invalido, envie o n??mero de ganhadores do sorteio")

############# UTILS #############

def registrar(user_id, fname): #Registrar novo usu??rio
    try:
        r = bdMap(1, "insert into clientes(cod, nome) values(%s, %s)", [user_id, fname], "insert")
        if r == "duplicate":
            app.send_message(user_id, "Usu??rio j?? cadastrado!")
        else:
            app.send_message(user_id, "Usu??rio cadastrado!")
            print(f"O usu??rio {fname}({user_id}) foi registrado\n")
    except Exception as errorrg:
        print(errorrg)


def cupom(nome, user_id, sorteio=None, fname=None, ind=False, indicado=None): #Gerar e registrar cupons de sorteio

    if sorteio is None and ind and indicado:
        r = bdMap(4, "insert into indicados(indicante, n_indicado, indicado) values(%s, %s, %s)", [user_id, fname, indicado], "insert")
        if r != "duplicate":
            sorts_dor = [s[3] for s in bdMap(3, "select * from cupons where user_cod=%s", [user_id])]
            sorts_ado = [s[3] for s in bdMap(3, "select * from cupons where user_cod=%s", [indicado])]

            if len(sorts_ado) > 0:
                msg_dor = f"Parab??ns! Por indicar {fname}, voc?? ganhou cupons para os seguinte(s) sorteio(s):\n\n"
                msg_ado = "Voc?? ganhou cupons para os seguinte(s) sorteio(s):\n\n"

                for sort in sorts_dor:
                    cupons = [c[4] for c in bdMap(3, "select * from cupons where user_cod=%s and sorteio=%s", [user_id, sort])]
                    num = gerador(exclude=cupons)

                    bdMap(3, "insert into cupons(nome, user_cod, sorteio, cupom) values(%s, %s, %s, %s)", [nome, user_id, sort, num], "insert")
                    
                    msg_dor += f"**{sort}**: {num}\n"

                for sort in sorts_ado:
                    cupons = [c[4] for c in bdMap(3, "select * from cupons where user_cod=%s and sorteio=%s", [indicado, sort])]
                    num = gerador(exclude=cupons)

                    bdMap(3, "insert into cupons(nome, user_cod, sorteio, cupom) values(%s, %s, %s, %s)", [fname, indicado, sort, num], "insert")

                    msg_ado += f"**{sort}**: {num}\n"
                    

                app.send_message(indicado, "Obrigado por aceitar a indica????o!\n\n"+msg_ado)
                print(f"O usu??rio {fname} registrou {user_id} como indicante --> /indica\n")

                app.send_message(user_id, msg_dor)
            
            else:
                app.send_message(indicado, "Para registrar um indicador, participe pelo menos de um sorteio!")

        else:
            app.send_message(indicado, "voc?? n??o pode usar mais de um c??digo de indica????o!")

    else:
        if not limite(user_id, int(sorteio[:3]) - 150):
            ds = bdMap(2, "select criador from sorteios where id=%s", [int(sorteio[:3]) - 150])[0][0]
            if user_id != ds:
                rg = bdMap(5, "select regras from regras where sorteio=%s", [int(sorteio[:3]) - 150])
                if not participa(nome, user_id, int(sorteio[:3]) - 150):
                    num = gerador()
                    bdMap(3, "insert into cupons(nome, user_cod, sorteio, cupom) values(%s, %s, %s, %s)", [nome, user_id, int(sorteio[:3]) - 150, num], "insert")
                    if len(rg) != 0:
                        app.send_message(user_id, "Regras do sorteio: \n\n"+rg[0][0])
                    app.send_message(user_id, f"Seu cupom para o sorteio {sorteio[4:]} ?? {num}")

                else:
                    if len(rg) != 0:
                        app.send_message(user_id, "Regras do sorteio: \n\n"+rg[0][0])
                    app.send_message(user_id, f"Voc?? j?? possui cupom(ns) desse sorteio.\n\nPara receber mais, indique para amigos! Voc?? pode indicar para at?? 10 amigos\n\nSeu c??digo de indica????o:\n```{user_id}```")
                    app.send_message(user_id, f"Est?? rolando sorteio no @Meusorteiobot!\n\nFa??a o seu cadastro e digite meu c??digo de indica????o\n\nDigite ```/indica {user_id}``` para participar!")    
            else:
                app.send_message(user_id, "Voc?? n??o pode participar do pr??prio sorteio!")

def gerador(mx=10000, exclude=None):
    num = rd(1, mx)

    if exclude:
        while num in exclude:
            num = rd(1, mx)

    return num

def ganhador(dono, sorteio): #Define o vencedor do sorteio
    parts = {}
    wins = []
    todos = bdMap(3, "select * from cupons where sorteio=%s", [sorteio[0]])
    loop = bdMap(2, "select ganhadores from sorteios where id=%s", [sorteio[0]])[0][0]
    nums = [n[4] for n in bdMap(3, "select * from cupons where sorteio=%s", [sorteio[0]])]

    if len(nums) > 0:
        for n in range(loop):
            ns = choice(nums)
            wins.append(ns)
            nums.append(ns)
            

        for part in todos:
            cp_id, nome, user_id, sort, cp = part

            if nome not in parts.keys():
                parts[nome] = {"uid": user_id, "cps": []}
            
            if cp not in parts[nome]["cps"]:
                parts[nome]["cps"].append(cp)

        msgd = f"{'O vencedor' if len(wins) == 1 else 'os vencedores'} do sorteio {'??:' if len(wins) == 1 else 's??o:'}\n\n"
        for k, v in parts.items():
            for win in wins:
                if win in v["cps"]:
                    username = app.get_chat(v["uid"]).username
                    dname = app.get_chat(dono).username
                    msgd += f"**@{username} com o cupom {win}**\n"

                    app.send_message(v["uid"], f"Parab??ns, voc?? venceu o sorteio {sorteio[0] + 150} - {sorteio[1]} com o cupom {win}!\n\nEntre em contato com @{dname}.")
                else:
                    app.send_message(v["uid"], f"O sorteio {sorteio[0] + 150} - {sorteio[1]} foi finalizado, infelizmente seu cupom n??o foi sorteado")
            app.send_message(dono, msgd, parse_mode=ParseMode.MARKDOWN)
        
        deleteSort(dono, dname, sorteio[0])
    
    else:
        app.send_message(dono, "Infelizmente esse sorteio n??o possui nenhum participante")

def deleteSort(user_id, fname, sorteio):
    sort_id, sort_name = sorteio
    bdMap(5, "delete from regras where sorteio=%s", [sort_id], "delete")
    bdMap(3, "delete from cupons where sorteio=%s", [sort_id], "delete")
    bdMap(2, "delete from sorteios where id=%s", [sort_id], "delete")

    app.send_message(user_id, f"O sorteio {sort_name} foi removido!")

    print(f"O usu??rio {fname}({user_id}) removeu o sorteio {sort_name} --> /rmSorteio\n")


def participa(nome, user_id, sorteio): #Verificar se um usu??rio j?? particia de um sorteio
    pt = [p[-1] for p in bdMap(3, "select * from cupons where user_cod=%s and sorteio=%s", [user_id, sorteio])]

    if len(pt) == 0:
        return False

    else:
        return True

def limite(indicante, sorteio):
    ind = bdMap(4, "select * from cupons where user_cod=%s and sorteio=%s", [indicante, sorteio])

    if len(ind) < 10:
        return False
    
    else:
        return True

def bdMap(c, sql, var=None,  method="select"): #Intera????es com banco de dados

    if not con.is_connected():
        con.close()
        bd()

    cursors = {
        1: cur1,
        2: cur2,
        3: cur3,
        4: cur4,
        5: cur5
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

@app.on_callback_query(filters.regex("^sort\S")) #Resposta para bot??o de registrar sorteio
def callSort(bot, call):
    user_id = call.from_user.id
    nome = call.from_user.first_name
    sorteio = call.data[5:]

    cupom(nome, user_id, sorteio=sorteio)

@app.on_callback_query(filters.regex("^win\S")) #Resposta para escolher vencedor do sorteio
def callWin(bot, call):
    sorteio = [int(call.data[4:7]) - 150, call.data[8:]]
    user_id = call.from_user.id

    ganhador(user_id, sorteio)

@app.on_callback_query(filters.regex("^regras\S"))
def callRegras(bot, call):
    user_id = call.from_user.id
    sorteio = [int(call.data[7:10]) - 150, call.data[11:]]

    add_regra[str(user_id)] = sorteio
    app.send_message(user_id, f"Envie as regras para o sorteio {sorteio[1]}")

@app.on_callback_query(filters.regex("^rmsort\S"))
def callDelete(bot, call):
    user_id = call.from_user.id
    fname = call.from_user.first_name
    sorteio = [int(call.data[7:10]) - 150, call.data[11:]]
    
    deleteSort(user_id, fname, sorteio)

@app.on_callback_query(filters.regex("^help_sortear")) #Resposta para bot??o sortear do help
def callSotear(bot, call):
    escSortear(bot, call.message)

@app.on_callback_query(filters.regex("^help_sorteios")) #Resposta para bot??o sorteios do help
def callSorteios(bot, call):
    sorteios(bot, call.message)

@app.on_callback_query(filters.regex("^help_cupons")) #Resposta para bot??o cupons do help
def callCupons(bot, call):
    consultarCp(bot, call.message)

@app.on_callback_query(filters.regex("^help_regsorteio")) #Resposta para bot??o Registrar Sorteio do help
def callRegSort(bot, call):
    rSorteio(bot, call.message)
    
@app.on_callback_query(filters.regex("^help_rmsorteio")) #Resposta para bot??o Remover Sorteio do help
def callRmSort(bot, call):
    rmSorteio(bot, call.message)

@app.on_callback_query(filters.regex("^help_atregras"))
def callRg(bot, call):
    regras(bot, call.message)

@app.on_callback_query(filters.regex("^help_ind")) #Resposta para bot??o Indica????o do help
def callInd(bot, call):
    user_id = call.from_user.id
    btns = [
        [InlineKeyboardButton("Registrar indica????o", callback_data="frescind_rgcodigo")],
        [InlineKeyboardButton("Meu c??digo", callback_data="frescind_mycodigo")],
        ]

    markup = InlineKeyboardMarkup(btns)
    app.send_message(user_id, "Escolha uma op????o", reply_markup=markup)

@app.on_callback_query(filters.regex("^frescind_rgcodigo"))
def callRgcodigo(bot, call):
    indica(bot, call.message)

@app.on_callback_query(filters.regex("^frescind_mycodigo"))
def callMycodigo(bot, call):
    user_id = call.from_user.id
    app.send_message(user_id, f"Est?? rolando sorteio no @Meusorteiobot!\n\nFa??a o seu cadastro e digite meu c??digo de indica????o\n\nDigite ```/indica {user_id}``` para participar!")

if __name__ == "__main__":
    bd()
    print("+----------------+\n"
          "|  BOT INICIADO  |\n"
          "+----------------+\n")
    app.run()
