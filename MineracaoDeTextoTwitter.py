#Importando as bibliotecas  para criar api com o twiiter, gerar o json
from tweepy.streaming import StreamListener #faz o stream dos dados do twitter
from tweepy import OAuthHandler #cria autenticação com a api do twitter
from tweepy import Stream
from sklearn.feature_extraction.text import CountVectorizer
import pandas as pd
from pymongo import MongoClient
import json

#Conectar api do twitter
autenticacao = OAuthHandler("RwWtjCIKdma3Pq7UlSrqtxHkX","i7NIqviWhu0UCl8PdTjxenKg4pcv0wrUI170w6mLScZ8IOxKjX")
autenticacao.set_access_token("263161626-iwqNErk36mVXTjKnrnfHUUC6ChdJh5fRUx8omOkO","MPG3T9Q55SvxjQq1XLUrCrf5CA7LgL4lLBjFBZKrceI7P")

#Cria conexão com o servidor do mongo db
Conectar = MongoClient('localhost', 27017)

#Criando o banco de dados TweeterAnalitcs
Criar_Banco = Conectar.TweeterAnalytcs

#Criar uma coleção para Coreia do Norte
#Criar_Colecao = Criar_Banco.create_collection('Tweets')

#Conectar a colecao

Conectar_colecao = Criar_Banco.Tweets

#Realizar scream dos tweets

class MyListener(StreamListener):
    def on_data(self,dados):
        tweet = json.loads(dados)
        id_str = tweet["id_str"]
        created_at = tweet["created_at"]
        text = tweet["text"]
        user = tweet["user"]["screen_name"]
        foto = tweet["user"]["profile_image_url"]
        documento = {"id_str":id_str,"created_at":created_at,"text":text,"user":user,"foto":foto}
        Inserir = Conectar_colecao.insert_one(documento).inserted_id
        print(documento)
        return True
    
def PopularBase():
    #Criar objeto para o scream
    mylistener = MyListener()
    #faz a autenticacao e obtem os dados twiiter a partir da api
    myStream = Stream(autenticacao, listener = mylistener)
    #definir as palavras chaves
    keywords = ['Geddel']
    #fazer o scream dos dados
    myStream.filter (track = keywords)
    
def CriarDataFrame():
    #Criar um data sete
    dataset = []
    for item in Conectar_colecao.find():
        dicionario = {"Autor":item["user"],
                      "Data_de_Criacao":item["created_at"],
                      "Tweet":item["text"],
                      "Foto":item["foto"],
                     }
        dataset.append(dicionario)
    #criar dataframe
    Data_Frame = pd.DataFrame(dataset)
    #imprimir dataframe
    return Data_Frame

def FiltrarPerfil():
    ContarVetor = CountVectorizer()
    Count_matrix = ContarVetor.fit_transform(CriarDataFrame().Autor)
    
    word_count = pd.DataFrame(ContarVetor.get_feature_names(), columns=["Autores"])
    word_count["Count_tweets"] = Count_matrix.sum(axis=0).tolist()[0]
    word_count = word_count.sort_values("Count_tweets", ascending=False).reset_index(drop=True)
    print(word_count[:1000])
    
    filtro = input("Informe o perfil:")
    #Criar um data sete
    dataset = []
    for item in Conectar_colecao.find():
        if item["user"] == filtro:
            dicionario = {"Autor":item["user"],
                          "Data_de_Criacao":item["created_at"],
                          "Tweet":item["text"],
                          "Foto":item["foto"],
                         }
            dataset.append(dicionario)
    #criar dataframe
    Data_Frame = pd.DataFrame(dataset)
    #imprimir dataframe
    return Data_Frame

FiltrarPerfil()