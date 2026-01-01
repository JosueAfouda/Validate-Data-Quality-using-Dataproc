# Tutoriel : Valider la Qualité des Données d'un Pipeline Batch avec Spark Serverless sur Google Cloud

Ce tutoriel vous guidera à travers les étapes pour lancer une tâche de validation de données (Data Quality) en utilisant un script PySpark sur Dataproc Serverless. Nous allons corriger une erreur de permission courante, exécuter le job, et vérifier les résultats en inspectant les données valides écrites dans BigQuery et les données invalides mises de côté dans une "Dead Letter Queue" sur Google Cloud Storage.

Ce guide est conçu pour les débutants.

## Prérequis

*   Un projet Google Cloud avec la facturation activée.
*   L'outil `gcloud` (Google Cloud CLI) installé et configuré.
*   Un script PySpark (`customer_dq.py` dans notre cas) qui effectue les vérifications de qualité. Ce script est déjà stocké dans un bucket Cloud Storage.
*   Un bucket Cloud Storage pour les dépendances du job.
*   Un bucket Cloud Storage pour servir de "Dead Letter Queue" (DLQ) pour stocker les enregistrements qui ont échoué à la validation.
*   Un ensemble de données (dataset) BigQuery prêt à recevoir les données propres.

## Étape 1 : Configuration des Permissions (Le Problème et la Solution)

Lors de l'exécution de jobs sur Google Cloud, les services ont besoin d'autorisations pour interagir les uns avec les autres. Une erreur fréquente est que le service qui exécute le code (ici, Dataproc) n'a pas les droits nécessaires pour agir sur d'autres ressources (comme les machines virtuelles).

L'erreur que nous avons rencontrée initialement indiquait un problème avec un "Service Account". Un Service Account est une identité spéciale qu'un service ou une application peut utiliser pour s'authentifier et obtenir des autorisations.

Nous allons donner les permissions nécessaires au Service Account par défaut de Compute Engine, qui est souvent utilisé par Dataproc pour exécuter les tâches.

1.  **Récupérez l'ID de votre projet**. Vous en aurez besoin pour les commandes suivantes.
    ```bash
    gcloud config get-value project
    ```
    *Sortie attendue :*
    ```
    qwiklabs-gcp-00-f7f32780700e
    ```

2.  **Identifiez votre Service Account**. Le Service Account par défaut de Compute Engine suit le format `[NUMÉRO_PROJET]-compute@developer.gserviceaccount.com`. Le numéro de notre projet est `952378983008`.

3.  **Attribuez le rôle `Dataproc Worker` au Service Account**. Ce rôle contient les permissions nécessaires pour que les nœuds Dataproc exécutent des jobs. Remplacez l'ID du projet et le nom du Service Account si les vôtres sont différents.

    ```bash
    gcloud projects add-iam-policy-binding qwiklabs-gcp-00-f7f32780700e \
      --member="serviceAccount:952378983008-compute@developer.gserviceaccount.com" \
      --role="roles/dataproc.worker"
    ```
    *Sortie attendue (un extrait) :*
    ```
    Updated IAM policy for project [qwiklabs-gcp-00-f7f32780700e].
    bindings:
    - members:
      - serviceAccount:952378983008-compute@developer.gserviceaccount.com
      role: roles/dataproc.worker
    ...
    ```

## Étape 2 : Lancement du Job de Qualité des Données

Maintenant que les permissions sont corrigées, nous pouvons soumettre notre job PySpark. La commande originale a échoué ; nous allons la ré-exécuter en ajoutant le flag `--service-account` pour dire explicitement à Dataproc quelle identité utiliser.

Voici la commande complète et détaillée :

```bash
gcloud dataproc batches submit pyspark gs://qwiklabs-gcp-00-f7f32780700e-main-bucket/scripts/customer_dq.py \
     --version=2.1 \
     --batch="customer-dq-job-$(date +%s)" \
     --region=us-central1 \
     --subnet=projects/qwiklabs-gcp-00-f7f32780700e/regions/us-central1/subnetworks/spark-subnet \
     --deps-bucket=gs://qwiklabs-gcp-00-f7f32780700e-main-bucket \
     --service-account="952378983008-compute@developer.gserviceaccount.com" \
     -- \
     customer_data_clean.valid_customers
```

**Explication des paramètres :**
*   `pyspark ...`: Indique que nous soumettons un job PySpark et spécifie le chemin vers le script sur Cloud Storage.
*   `--version`: Spécifie la version de Dataproc.
*   `--batch`: Donne un nom unique à notre exécution de job.
*   `--region`: La région Google Cloud où exécuter le job.
*   `--subnet`: La configuration réseau. Les jobs Spark s'exécutent sur des ressources qui ont besoin d'être connectées à un réseau.
*   `--deps-bucket`: Un bucket utilisé par Dataproc pour stocker les dépendances ou les fichiers temporaires.
*   `--service-account`: **Le point clé de notre correction !** Nous indiquons explicitement d'utiliser le Service Account que nous avons configuré à l'étape 1.
*   `--`: Un séparateur qui indique que les arguments suivants sont destinés à notre script PySpark, et non à `gcloud`.
*   `customer_data_clean.valid_customers`: L'argument passé à notre script, qui est le nom de la table BigQuery de destination pour les données propres.

Après avoir lancé la commande, vous verrez les logs de l'exécution. Le job devrait se terminer avec succès.

## Étape 3 : Vérification des Résultats

Un job qui se termine, c'est bien. Un job qui a fait ce qu'on attendait, c'est mieux ! Nous allons maintenant vérifier les sorties.

### A. Vérifier les données valides dans BigQuery

Notre script a été conçu pour écrire les enregistrements valides dans la table `valid_customers`.

1.  **Comptez le nombre d'enregistrements propres.**
    ```bash
    bq query \
      --use_legacy_sql=false \
      'SELECT count(*) as total_clean_records FROM `customer_data_clean.valid_customers`;'
    ```
    *Sortie :*
    ```
    +---------------------+
    | total_clean_records |
    +---------------------+
    |                 816 |
    +---------------------+
    ```
    Cela nous indique que 816 enregistrements ont passé les règles de validation.

2.  **Inspectez un échantillon de ces enregistrements.**
    ```bash
    bq query \
      --use_legacy_sql=false \
      'SELECT * FROM `customer_data_clean.valid_customers` LIMIT 10;'
    ```
    *Sortie :*
    ```
    +-----+------------+-----------+-----------------------------+
    | id  | first_name | last_name |            email            |
    +-----+------------+-----------+-----------------------------+
    |  20 | Aaron      | Bowen     | teresa28@example.org        |
    | 439 | Aaron      | Schroeder | simonstephen@example.net    |
    | 595 | Aaron      | Powers    | conleyelizabeth@example.org |
    ...
    +-----+------------+-----------+-----------------------------+
    ```
    Les données semblent complètes et bien formatées, avec un ID, un prénom, un nom et une adresse email valide.

### B. Examiner les données invalides (Dead Letter Queue)

Notre script a été programmé pour envoyer les enregistrements qui ne respectent pas les règles de qualité vers un fichier CSV dans un bucket Cloud Storage (notre DLQ).

Examinons le contenu de ces fichiers d'erreurs.

```bash
gcloud storage cat gs://qwiklabs-gcp-00-f7f32780700e-dlq-bucket/errors/*.csv | head -n 11
```
*Sortie :*
```
id,first_name,last_name,email
2,Joy,Gardner,fjohnson
,Jeffrey,Lawrence,blakeerik@example.com
7,Christian,Carter,barbara10
,Jeremy,Roberts,wyattmichelle@example.com
,John,Ford,veronica83@example.net
19,Karen,Mack,daniel62
21,Monica,Evans,ericfarmer
22,Stephanie,Nielsen,georgetracy
23,Sarah,Koch,john39
,Corey,Pearson,novaksara@example.org
```
En analysant ce fichier, nous pouvons comprendre pourquoi ces enregistrements ont été rejetés :
*   Certaines lignes n'ont pas de valeur dans la colonne `id`.
*   D'autres ont une adresse email invalide qui n'est pas complète (ex: `fjohnson` au lieu de `fjohnson@example.com`).

Notre pipeline de qualité de données a parfaitement fonctionné !

## Conclusion

Félicitations ! Vous avez :
1.  Diagnostiqué et résolu un problème de permissions de Service Account, une compétence essentielle sur le cloud.
2.  Lancé avec succès un job Spark serverless avec `gcloud dataproc batches submit`.
3.  Vérifié la sortie du job en interrogeant BigQuery pour les données propres et Cloud Storage pour les données invalides.

Vous avez maintenant une compréhension pratique de la mise en place d'un pipeline de validation de données robuste et serverless sur Google Cloud.
