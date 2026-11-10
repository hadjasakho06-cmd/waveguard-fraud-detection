# WaveGuard — Simulateur de transactions Mobile Money

## Présentation

WaveGuard est un projet de détection de transactions Mobile Money suspectes en temps réel. Ce dépôt contient le script Python `producer.py`, chargé de générer des transactions simulées et de les publier dans un topic Apache Kafka.

Le producteur permet de créer un flux de transactions ordinaires ainsi que des rafales de transactions associées à des comptes identifiés comme suspects. Ces données peuvent ensuite servir de flux d'entrée à un pipeline de traitement et de détection de fraude.

> **Périmètre de ce dépôt :** le code fourni ici implémente la génération et la publication des transactions (producer Kafka). Il ne contient pas, à lui seul, le consommateur Kafka, le traitement Spark Structured Streaming, le stockage Parquet ou le tableau de bord Grafana.

## Fonctionnalités du producteur

- Génération de transactions Mobile Money au format JSON.
- Création d'identifiants uniques pour les transactions.
- Ajout d'un horodatage UTC.
- Simulation de comptes ordinaires et de trois comptes suspects.
- Simulation de montants ordinaires et de montants élevés associés aux transactions suspectes.
- Génération de rafales de huit transactions depuis un même compte suspect.
- Publication dans le topic Kafka `transactions`.
- Utilisation de `sender_id` comme clé Kafka afin de regrouper les transactions d'un même expéditeur dans la même partition, selon le partitionneur et la configuration du topic.
- Affichage du topic, de la partition, de l'offset et de la clé après livraison.

## Technologies et dépendances

- Python 3.9 ou supérieur
- Apache Kafka
- `confluent-kafka`
- `Faker`

Les modules `json`, `time`, `random`, `uuid` et `datetime` utilisés par le script font partie de la bibliothèque standard de Python.

## Structure du dépôt

```text
waveguard/
├── producer.py
├── requirements.txt
└── README.md
```

## Prérequis

Avant de lancer le producteur, assurez-vous que :

1. Python est installé.
2. Un broker Apache Kafka est démarré et accessible à l'adresse `localhost:9092`.
3. Le topic Kafka `transactions` existe.

Le script ne crée pas automatiquement le topic Kafka.

### Créer le topic Kafka

Avec les outils en ligne de commande Kafka disponibles dans votre installation, vous pouvez créer le topic ainsi :

```bash
kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic transactions \
  --partitions 3 --replication-factor 1
```

Cet exemple crée un topic de trois partitions avec un facteur de réplication de un, adapté à un environnement local de démonstration. Ajustez ces paramètres selon votre installation.

## Installation

Clonez le dépôt :

```bash
git clone <URL_DU_DEPOT>
cd <NOM_DU_DEPOT>
```

Remplacez `<URL_DU_DEPOT>` et `<NOM_DU_DEPOT>` par l'adresse et le nom réels de votre dépôt GitHub.

Créez et activez un environnement virtuel.

**Linux / macOS :**

```bash
python3 -m venv .venv
source .venv/bin/activate
```

**Windows PowerShell :**

```powershell
py -m venv .venv
.venv\Scripts\Activate.ps1
```

Installez les dépendances :

```bash
pip install -r requirements.txt
```

## Exécution

Démarrez le broker Kafka et vérifiez que le topic `transactions` existe, puis lancez :

```bash
python producer.py
```

Le terminal affiche les confirmations de livraison des messages, notamment le topic, la partition, l'offset et la clé utilisée.

Pour arrêter le producteur, utilisez `Ctrl+C`. Le script tente alors de vider les messages encore en attente avec `producer.flush()`.

## Format des transactions

Chaque message est publié au format JSON avec les champs suivants :

| Champ | Description |
|---|---|
| `transaction_id` | Identifiant unique de la transaction |
| `timestamp` | Date et heure de génération au format ISO 8601 en UTC |
| `sender_id` | Identifiant du compte émetteur |
| `receiver_id` | Identifiant du compte destinataire |
| `amount_fcfa` | Montant simulé en FCFA |
| `transaction_type` | Type : `P2P`, `PAIEMENT_MARCHAND` ou `RETRAIT` |
| `location` | Localité simulée |
| `is_flagged` | Indicateur de vérité terrain : `true` pour les comptes de la liste des comptes suspects |

### Règles de simulation présentes dans le code

- 50 identifiants de comptes sont définis (`SN_0001` à `SN_0050`).
- Trois comptes sont déclarés suspects : `SN_0042`, `SN_0007` et `SN_0013`.
- Les transactions ordinaires utilisent un montant compris entre 500 et 150 000 FCFA.
- Les transactions générées avec le mode fraude utilisent un montant compris entre 800 000 et 2 000 000 FCFA.
- Une rafale contient huit transactions émises depuis le même compte, avec un délai de 50 millisecondes entre deux envois.
- À chaque cycle principal, le script a 10 % de probabilité de déclencher une rafale frauduleuse ; sinon, il génère une transaction ordinaire.

Ces règles sont des paramètres de simulation et ne constituent pas, à elles seules, un modèle validé de détection de fraude réelle.

## Configuration

Les paramètres de connexion et le nom du topic sont définis au début de `producer.py` :

```python
BROKER = 'localhost:9092'
TOPIC = 'transactions'
```

Adaptez `BROKER` si votre broker Kafka est hébergé à une autre adresse.

La clé Kafka est définie à partir de `sender_id`. Avec le partitionneur Kafka, cela permet normalement d'envoyer les messages d'un même expéditeur vers la même partition, ce qui est utile pour analyser la vélocité des transactions par compte.

## Limites et pistes d'amélioration

- Externaliser l'adresse du broker et le nom du topic dans un fichier de configuration ou des variables d'environnement.
- Ajouter des paramètres de ligne de commande pour contrôler le débit, les comptes et les règles de simulation.
- Ajouter des tests automatisés pour la génération et la validation des transactions.
- Développer le consommateur Kafka et le traitement de détection en streaming.
- Ajouter la persistance des alertes et leur visualisation dans les composants correspondants du projet.

## Auteur

**Racky Sakho SOW**  
GitHub : [@hadjasakho06-cmd](https://github.com/hadjasakho06-cmd)

---

Projet académique — Big Data, streaming et simulation de transactions Mobile Money.
