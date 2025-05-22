from Clients.AnalyserClient import AnalyserClient

BROKER = 'localhost'
PORT   = 1883
client = AnalyserClient(client_id='analyser', host=BROKER, port=PORT)
client.run()