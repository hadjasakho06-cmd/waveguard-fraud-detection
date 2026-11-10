# producer.py — Simulateur de transactions Mobile Money WaveGuard
from confluent_kafka import Producer
from faker import Faker
import json, time, random, uuid
from datetime import datetime, timezone

fake = Faker('fr_FR')
BROKER = 'localhost:9092'
TOPIC  = 'transactions'

# Liste de comptes (certains seront des fraudeurs)
ACCOUNTS = [f'SN_{i:04d}' for i in range(1, 51)]
FRAUD_ACCOUNTS = ['SN_0042', 'SN_0007', 'SN_0013']  # comptes suspects

conf = {'bootstrap.servers': BROKER}
producer = Producer(conf)

def delivery_report(err, msg):
    """Callback de livraison. Affiche aussi la clé (Key) utilisée, ce qui
    permet de vérifier (point 3) que toutes les transactions d'un même
    sender_id tombent bien dans la même partition."""
    if err:
        print(f'[ERREUR] Livraison échouée : {err}')
    else:
        print(f'[OK] Topic={msg.topic()} | Partition={msg.partition()} | '
              f'Offset={msg.offset()} | Key={msg.key().decode()}')

def generate_transaction(fraud=False):
    sender = random.choice(FRAUD_ACCOUNTS if fraud else ACCOUNTS)
    amount = random.randint(800_000, 2_000_000) if fraud else random.randint(500, 150_000)
    return {
        'transaction_id': str(uuid.uuid4()),
        'timestamp':      datetime.now(timezone.utc).isoformat(),
        'sender_id':      sender,
        'receiver_id':    random.choice(ACCOUNTS),
        'amount_fcfa':    amount,
        'transaction_type': random.choice(['P2P', 'PAIEMENT_MARCHAND', 'RETRAIT']),
        'location':       random.choice(['Dakar', 'Thiès', 'Saint-Louis', 'Ziguinchor', 'Kaolack']),
        # Point 2 : champ vérité-terrain, True si le compte est un fraudeur connu
        'is_flagged':     sender in FRAUD_ACCOUNTS,
    }

def send_fraud_burst(sender):
    """Simule une attaque par vélocité :
    8 transactions en rafale depuis le même compte, espacées de 50ms."""
    print(f'\n[BURST] Rafale frauduleuse sur {sender} (8 tx / 50ms)')
    for _ in range(8):
        tx = generate_transaction(fraud=True)
        tx['sender_id'] = sender  # force le même compte pour toute la rafale
        producer.produce(
            TOPIC,
            key=tx['sender_id'].encode(),  # Point 3 : clé = partitionnement explicite par compte
            value=json.dumps(tx).encode(),
            callback=delivery_report
        )
        producer.poll(0)
        time.sleep(0.05)  # 50 ms entre chaque transaction du burst

print('=== WaveGuard Producer démarré ===  Ctrl+C pour arrêter')
try:
    while True:
        # Injection de fraude : 10% de chance de déclencher un burst frauduleux
        is_fraud = random.random() < 0.10

        if is_fraud:
            fraud_sender = random.choice(FRAUD_ACCOUNTS)
            send_fraud_burst(fraud_sender)
        else:
            tx = generate_transaction(fraud=False)
            producer.produce(
                TOPIC,
                key=tx['sender_id'].encode(),  # Point 3 : clé = partitionnement explicite par compte
                value=json.dumps(tx).encode(),
                callback=delivery_report
            )
            producer.poll(0)

        time.sleep(random.uniform(0.05, 0.3))   # ~5 à 20 tx/sec

except KeyboardInterrupt:
    print('Arrêt du producer...')
    producer.flush()
    
