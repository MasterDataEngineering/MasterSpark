from data_processing.processing_data import Rowprocessor
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, Row
import json


class TestProcessing:
    metadata_json = None
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .getOrCreate()
    processing = Rowprocessor(spark)
    schema_json = '{"fields":[{"metadata":{},"name":"data","nullable":true,"type":{"fields":[{"metadata":{},"name":"$type","nullable":true,"type":"string"},{"metadata":{},"name":"*applyingInfo","nullable":true,"type":"string"},{"metadata":{},"name":"*miniCompany","nullable":true,"type":"string"},{"metadata":{},"name":"*miniJob","nullable":true,"type":"string"},{"metadata":{},"name":"*savingInfo","nullable":true,"type":"string"},{"metadata":{},"name":"applicant","nullable":true,"type":{"fields":[{"metadata":{},"name":"$type","nullable":true,"type":"string"},{"metadata":{},"name":"*miniProfile","nullable":true,"type":"string"},{"metadata":{},"name":"emailAddress","nullable":true,"type":"string"},{"metadata":{},"name":"otherEmailAddresses","nullable":true,"type":{"containsNull":true,"elementType":"integer","type":"array"}}],"type":"struct"}},{"metadata":{},"name":"basicCompanyInfo","nullable":true,"type":{"fields":[{"metadata":{},"name":"$type","nullable":true,"type":"string"},{"metadata":{},"name":"*followingInfo","nullable":true,"type":"string"},{"metadata":{},"name":"*miniCompany","nullable":true,"type":"string"},{"metadata":{},"name":"followerCount","nullable":true,"type":"long"},{"metadata":{},"name":"headquarters","nullable":true,"type":"string"}],"type":"struct"}},{"metadata":{},"name":"closed","nullable":true,"type":"boolean"},{"metadata":{},"name":"companyApplyPostUrl","nullable":true,"type":"integer"},{"metadata":{},"name":"companyApplyUrl","nullable":true,"type":"string"},{"metadata":{},"name":"companyDescription","nullable":true,"type":"string"},{"metadata":{},"name":"companyName","nullable":true,"type":"string"},{"metadata":{},"name":"description","nullable":true,"type":"string"},{"metadata":{},"name":"employmentStatus","nullable":true,"type":"string"},{"metadata":{},"name":"encryptedPricingParams","nullable":true,"type":"string"},{"metadata":{},"name":"entityInfo","nullable":true,"type":{"fields":[{"metadata":{},"name":"$type","nullable":true,"type":"string"},{"metadata":{},"name":"objectUrn","nullable":true,"type":"string"},{"metadata":{},"name":"trackingId","nullable":true,"type":"string"}],"type":"struct"}},{"metadata":{},"name":"entityUrn","nullable":true,"type":"string"},{"metadata":{},"name":"experienceLevel","nullable":true,"type":"string"},{"metadata":{},"name":"flavors","nullable":true,"type":{"containsNull":true,"elementType":{"fields":[{"metadata":{},"name":"$type","nullable":true,"type":"string"},{"metadata":{},"name":"reason","nullable":true,"type":{"fields":[{"metadata":{},"name":"$type","nullable":true,"type":"string"},{"metadata":{},"name":"*currentCompany","nullable":true,"type":"string"},{"metadata":{},"name":"coworkers","nullable":true,"type":{"containsNull":true,"elementType":{"fields":[{"metadata":{},"name":"$type","nullable":true,"type":"string"},{"metadata":{},"name":"*miniProfile","nullable":true,"type":"string"},{"metadata":{},"name":"distance","nullable":true,"type":{"fields":[{"metadata":{},"name":"$type","nullable":true,"type":"string"},{"metadata":{},"name":"value","nullable":true,"type":"string"}],"type":"struct"}}],"type":"struct"},"type":"array"}},{"metadata":{},"name":"totalNumberOfPastCoworkers","nullable":true,"type":"long"}],"type":"struct"}}],"type":"struct"},"type":"array"}},{"metadata":{},"name":"heroImage","nullable":true,"type":{"fields":[{"metadata":{},"name":"$type","nullable":true,"type":"string"},{"metadata":{},"name":"id","nullable":true,"type":"string"}],"type":"struct"}},{"metadata":{},"name":"industries","nullable":true,"type":{"containsNull":true,"elementType":"string","type":"array"}},{"metadata":{},"name":"isLinkedInRouting","nullable":true,"type":"boolean"},{"metadata":{},"name":"jobFunctions","nullable":true,"type":{"containsNull":true,"elementType":"string","type":"array"}},{"metadata":{},"name":"jobPoster","nullable":true,"type":"integer"},{"metadata":{},"name":"linkedInRouting","nullable":true,"type":"boolean"},{"metadata":{},"name":"numberOfApplicants","nullable":true,"type":"long"},{"metadata":{},"name":"numberOfViewers","nullable":true,"type":"long"},{"metadata":{},"name":"sections","nullable":true,"type":{"fields":[{"metadata":{},"name":"$type","nullable":true,"type":"string"},{"metadata":{},"name":"highlights","nullable":true,"type":{"fields":[{"metadata":{},"name":"$type","nullable":true,"type":"string"},{"metadata":{},"name":"items","nullable":true,"type":{"containsNull":true,"elementType":{"fields":[{"metadata":{},"name":"$type","nullable":true,"type":"string"},{"metadata":{},"name":"item","nullable":true,"type":{"fields":[{"metadata":{},"name":"$type","nullable":true,"type":"string"},{"metadata":{},"name":"*followingInfo","nullable":true,"type":"string"},{"metadata":{},"name":"*miniCompany","nullable":true,"type":"string"},{"metadata":{},"name":"attributedText","nullable":true,"type":{"fields":[{"metadata":{},"name":"$type","nullable":true,"type":"string"},{"metadata":{},"name":"attributes","nullable":true,"type":{"containsNull":true,"elementType":{"fields":[{"metadata":{},"name":"$type","nullable":true,"type":"string"},{"metadata":{},"name":"attributeKindUnion","nullable":true,"type":{"fields":[{"metadata":{},"name":"bold","nullable":true,"type":{"fields":[{"metadata":{},"name":"$type","nullable":true,"type":"string"}],"type":"struct"}},{"metadata":{},"name":"italic","nullable":true,"type":{"fields":[{"metadata":{},"name":"$type","nullable":true,"type":"string"}],"type":"struct"}},{"metadata":{},"name":"lineBreak","nullable":true,"type":{"fields":[{"metadata":{},"name":"$type","nullable":true,"type":"string"}],"type":"struct"}},{"metadata":{},"name":"list","nullable":true,"type":{"fields":[{"metadata":{},"name":"$type","nullable":true,"type":"string"},{"metadata":{},"name":"ordered","nullable":true,"type":"boolean"}],"type":"struct"}},{"metadata":{},"name":"listItem","nullable":true,"type":{"fields":[{"metadata":{},"name":"$type","nullable":true,"type":"string"}],"type":"struct"}},{"metadata":{},"name":"paragraph","nullable":true,"type":{"fields":[{"metadata":{},"name":"$type","nullable":true,"type":"string"}],"type":"struct"}},{"metadata":{},"name":"underline","nullable":true,"type":{"fields":[{"metadata":{},"name":"$type","nullable":true,"type":"string"}],"type":"struct"}}],"type":"struct"}},{"metadata":{},"name":"length","nullable":true,"type":"long"},{"metadata":{},"name":"start","nullable":true,"type":"long"},{"metadata":{},"name":"type","nullable":true,"type":{"fields":[{"metadata":{},"name":"$type","nullable":true,"type":"string"},{"metadata":{},"name":"ordered","nullable":true,"type":"boolean"}],"type":"struct"}}],"type":"struct"},"type":"array"}},{"metadata":{},"name":"text","nullable":true,"type":"string"}],"type":"struct"}},{"metadata":{},"name":"followerCount","nullable":true,"type":"long"},{"metadata":{},"name":"headquarters","nullable":true,"type":"string"},{"metadata":{},"name":"text","nullable":true,"type":"string"}],"type":"struct"}},{"metadata":{},"name":"itemInfo","nullable":true,"type":{"fields":[{"metadata":{},"name":"$type","nullable":true,"type":"string"},{"metadata":{},"name":"itemType","nullable":true,"type":"string"},{"metadata":{},"name":"trackingId","nullable":true,"type":"string"}],"type":"struct"}}],"type":"struct"},"type":"array"}}],"type":"struct"}}],"type":"struct"}},{"metadata":{},"name":"skillsDescription","nullable":true,"type":"integer"}],"type":"struct"}},{"metadata":{},"name":"included","nullable":true,"type":{"containsNull":true,"elementType":{"fields":[{"metadata":{},"name":"$type","nullable":true,"type":"string"},{"metadata":{},"name":"active","nullable":true,"type":"boolean"},{"metadata":{},"name":"activities","nullable":true,"type":"integer"},{"metadata":{},"name":"applied","nullable":true,"type":"boolean"},{"metadata":{},"name":"appliedAt","nullable":true,"type":"integer"},{"metadata":{},"name":"appliedTime","nullable":true,"type":"integer"},{"metadata":{},"name":"backgroundImage","nullable":true,"type":{"fields":[{"metadata":{},"name":"$type","nullable":true,"type":"string"},{"metadata":{},"name":"artifacts","nullable":true,"type":{"containsNull":true,"elementType":{"fields":[{"metadata":{},"name":"$type","nullable":true,"type":"string"},{"metadata":{},"name":"expiresAt","nullable":true,"type":"long"},{"metadata":{},"name":"fileIdentifyingUrlPathSegment","nullable":true,"type":"string"},{"metadata":{},"name":"height","nullable":true,"type":"long"},{"metadata":{},"name":"width","nullable":true,"type":"long"}],"type":"struct"},"type":"array"}},{"metadata":{},"name":"rootUrl","nullable":true,"type":"string"}],"type":"struct"}},{"metadata":{},"name":"closed","nullable":true,"type":"integer"},{"metadata":{},"name":"customPronoun","nullable":true,"type":"integer"},{"metadata":{},"name":"dashCompanyUrn","nullable":true,"type":"string"},{"metadata":{},"name":"dashEntityUrn","nullable":true,"type":"string"},{"metadata":{},"name":"dashFollowingStateUrn","nullable":true,"type":"string"},{"metadata":{},"name":"dashSaveStateUrn","nullable":true,"type":"integer"},{"metadata":{},"name":"entityUrn","nullable":true,"type":"string"},{"metadata":{},"name":"firstName","nullable":true,"type":"string"},{"metadata":{},"name":"followerCount","nullable":true,"type":"long"},{"metadata":{},"name":"following","nullable":true,"type":"boolean"},{"metadata":{},"name":"followingCount","nullable":true,"type":"integer"},{"metadata":{},"name":"followingType","nullable":true,"type":"string"},{"metadata":{},"name":"lastName","nullable":true,"type":"string"},{"metadata":{},"name":"listDate","nullable":true,"type":"long"},{"metadata":{},"name":"listedAt","nullable":true,"type":"long"},{"metadata":{},"name":"location","nullable":true,"type":"string"},{"metadata":{},"name":"logo","nullable":true,"type":{"fields":[{"metadata":{},"name":"$type","nullable":true,"type":"string"},{"metadata":{},"name":"artifacts","nullable":true,"type":{"containsNull":true,"elementType":{"fields":[{"metadata":{},"name":"$type","nullable":true,"type":"string"},{"metadata":{},"name":"expiresAt","nullable":true,"type":"long"},{"metadata":{},"name":"fileIdentifyingUrlPathSegment","nullable":true,"type":"string"},{"metadata":{},"name":"height","nullable":true,"type":"long"},{"metadata":{},"name":"width","nullable":true,"type":"long"}],"type":"struct"},"type":"array"}},{"metadata":{},"name":"rootUrl","nullable":true,"type":"string"}],"type":"struct"}},{"metadata":{},"name":"memorialized","nullable":true,"type":"boolean"},{"metadata":{},"name":"name","nullable":true,"type":"string"},{"metadata":{},"name":"objectUrn","nullable":true,"type":"string"},{"metadata":{},"name":"occupation","nullable":true,"type":"string"},{"metadata":{},"name":"picture","nullable":true,"type":{"fields":[{"metadata":{},"name":"$type","nullable":true,"type":"string"},{"metadata":{},"name":"artifacts","nullable":true,"type":{"containsNull":true,"elementType":{"fields":[{"metadata":{},"name":"$type","nullable":true,"type":"string"},{"metadata":{},"name":"expiresAt","nullable":true,"type":"long"},{"metadata":{},"name":"fileIdentifyingUrlPathSegment","nullable":true,"type":"string"},{"metadata":{},"name":"height","nullable":true,"type":"long"},{"metadata":{},"name":"width","nullable":true,"type":"long"}],"type":"struct"},"type":"array"}},{"metadata":{},"name":"rootUrl","nullable":true,"type":"string"}],"type":"struct"}},{"metadata":{},"name":"publicIdentifier","nullable":true,"type":"string"},{"metadata":{},"name":"resumeDownloadUrl","nullable":true,"type":"integer"},{"metadata":{},"name":"resumeFileName","nullable":true,"type":"integer"},{"metadata":{},"name":"resumeFileType","nullable":true,"type":"integer"},{"metadata":{},"name":"saved","nullable":true,"type":"boolean"},{"metadata":{},"name":"savedAt","nullable":true,"type":"integer"},{"metadata":{},"name":"showcase","nullable":true,"type":"boolean"},{"metadata":{},"name":"standardizedPronoun","nullable":true,"type":"integer"},{"metadata":{},"name":"title","nullable":true,"type":"string"},{"metadata":{},"name":"trackingId","nullable":true,"type":"string"},{"metadata":{},"name":"trackingUrn","nullable":true,"type":"string"},{"metadata":{},"name":"universalName","nullable":true,"type":"string"},{"metadata":{},"name":"viewedByJobPosterAt","nullable":true,"type":"integer"}],"type":"struct"},"type":"array"}}],"type":"struct"}'

    def get_json(self):
        if self.metadata_json is None:
            new_schema = StructType.fromJson(json.loads(self.schema_json))
            input_path = "test/data.json"
            sparkDF = self.spark.read.json(input_path, schema=new_schema)
            self.metadata_json = sparkDF
        return self.metadata_json

    # Restore schema from json:

    def test_extract(self):
        data = self.get_json()
        process_data = self.processing.process_data(data)
        expected_results = [Row(company_name='Nextar ',
                                description="Nextar è una società specializzata nella consulenza in ambito IT con diverse sedi sul territorio nazionale (Milano, Roma, Torino, Napoli, Bari) e una filiale distaccata su Londra, partner di riferimento per aziende di diversi settori e dimensioni, dalle PMI alle multinazionali, alle quali propone servizi dedicati.Le principali aree di competenza sono: produzione e gestione di software, progetti IT in ambito ERP (SAP, SAP B1), Java e tecnologie Microsoft, WEB e Mobile in tutte le tecnologie, oltre alla consulenza IT. Il nostro obiettivo è diventare un punto di riferimento nel mondo della consulenza aziendale ed IT. Vogliamo creare valore per i nostri clienti essendo riconosciuti come un partner in grado di fornire servizi ad elevato valore aggiunto, con prezzi competitivi e di alta qualità. In un contesto molto dinamico ed in forte espansione siamo alla ricerca di una figura come:\nSviluppatore PL-SQLSi richiede una buona predisposizione al team working e ottime capacità comunicative e relazionali. Completano il profilo una forte motivazione, proattività e senso di responsabilità.\nMust have:La risorsa ricercata, inserita all'interno del Team di sviluppo software in un contesto dinamico e stimolante, ha maturato almeno 2 anni di esperienza.\nNice to have:Laurea (magistrale o triennale) in discipline tecniche o diploma di perito informatico o titolo equivalente.Buona conoscenza della lingua inglese.\nCosa comprende l’offerta:Contratto di lavoro a tempo indeterminato con livello e retribuzione commisurati all’effettiva esperienza maturata, o Partita Iva;Benefit: Carta di credito aziendale; aggiornamento professionale tramite piattaforma aziendale.\nSede di lavoro:Torino.\nNon saranno presi in considerazione i cv sprovvisti dell'autorizzazione al trattamento dei dati personali (D.Lgs 196/2003) e del regolamento europeo 2016/679. Le offerte si intendono rivolte a persone di entrambi i sessi (L. 903/77). Per ulteriori informazioni visitare il seguente link.",
                                employements_status='Full-time', experience_level='Mid-Senior level', site='TORINO',
                                industries=['{"item":"Information Technology & Services"}',
                                            '{"item":"Program Development"}'], job_title=None)]
        assert process_data.collect() == expected_results