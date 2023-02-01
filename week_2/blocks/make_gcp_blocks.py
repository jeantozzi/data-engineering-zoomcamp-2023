from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket

credentials_block = GcpCredentials(
    service_account_info={  "type": "service_account",
                            "project_id": "dtc-de-zoomcamp-jt",
                            "private_key_id": "d386e3b236ff67e88422601bfa5eb987aa69f9a1",
                            "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQCiNqBjaon7PAjI\nCKy7qGZuWEbrqo1ctdXegkPIkhYx9PsiU6VNUc+cbfGpm64yKrqxo6tASK1qbNcE\nieGm7F0cOnz7QxRLS2WMdMOn0fbyl9fTKpzQIZ6f1E++N2TJcAKYPiTid7vsPIkx\nV1fGLYXci3eJPsxMKa5f5Edm9tFS/fNdfNocbWWqsJsGGW0JSR4wF333EPXcoLCH\nfhHq7Zc+EuiAjS6hpLLeRYPMvpzdK5esCv6Mp3p5+0WDoomB1Z6/yXlAXdqWcrfJ\nPRXjIJSbSnmPH+RbPRonZpkwN+1MNYBM4jizot+MO/HWtbCG1z8tjHUsS90HCKuC\nF8fS3MH5AgMBAAECggEAFqV0t9xwXgxQF4kqgFpuznnxkMYzTR2vuYxLJU3L3naa\ntvyhh81jpC2vPuQWqjdP8Cvu5R8gYsrGLAKJ59F44E+EEloXk7l3eGf2xs+n3892\nGKuKz/3ZlOvBpEc8gWjrW9DVOm7H5B/BcYuFnAvP8+3epuEHCZ0KFKYcl9HegGzt\n5xmGWTAarOmebc/59Q1wcCGdORx9reeKTARNXEn97ggup/eSsOxsl/KoW9GaeD+O\n3T3WDazHM/aWuiM72R3/4KDrokZZFRSxBbv686jXtahxxA8wF7Wf4WTsJrZR5ufy\n4cXQGR1JwSQH5bG7vtTVVCszi6sXUtpmqwg+N9MjZwKBgQDSOHmnj5BT90hvOJ8/\nO5346VvphOuhPCPvk4dj4S6a7vh5fupYskSTPHPRTIcB+vx98jeFiitZ+t22ymOM\nePof4e0pW0somwOYkzq2uVXFLPOJUxAEyHuxNuLy3jDHcC9+eppL50Arj3WXkBWx\nnueBzNLZ4ZrM3/Jii/bQvFZw8wKBgQDFic+wNwmIXLQj7aR9mcFR3CSJIsLyJ8bh\nVn3BUEJJvfg1j1jBTNad8y1O4UBz15Ce2EMoxxa30V4WWUbhC4w3H4jXZRgyfqpB\nClxkIC/czibUWk8v5EvSpWweFWkoULsY9FOm72fBz7SgX2W3O/ih/+oeC16tW6eR\noGM2y7mcYwKBgQCavamHFt1FgWPXQtWt5QNugs/+P7J2t9837d+ePNZig39xZ2O3\nMFncC4axTOSgJ5EOYGpQKRmqHUhw0d+fOjnbmCFL7QCQ/jIScGWqtRgWkPUfY34K\nF009cEp0PeuoEsNTFYpYTvvkVLBZhV0CpxaAbDXU3gVlw38W3TIsCkaS0wKBgQCh\nYvkqh5yjKtIF9HaKIx8dKRU0cRECVFsY6NfvIwo1Qx5Ra97gdk11WCKxKjjFK928\n2QTtgNJftY8ABUpyPEP/GSVsjOya/H9Oig93whA4fricj3fYsdT8sSfG4Ek8pdBm\nD990KG4FmxOAXtA0RqvYdsOobjOYwKQDxxynvQPU7wKBgQCdHuL0bnMhtE7nqHn6\nMjphBXVRUVxbs+X1v6KQUG0NA0NLmUt9Wsb97STMyAMtJVpKzZfSRce+sHJJsVP5\nLNiaC/Lyn6rnL6QH9DskhhrEmBfr/Z4wZEmvVQ2th2tGb0D9CcxDlMQRqsmCINw2\nCvOy7kiSb8CWpjWVNDNmnyLmcg==\n-----END PRIVATE KEY-----\n",
                            "client_email": "dtc-de-user@dtc-de-zoomcamp-jt.iam.gserviceaccount.com",
                            "client_id": "101053365858642771005",
                            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                            "token_uri": "https://oauth2.googleapis.com/token",
                            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                            "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/dtc-de-user%40dtc-de-zoomcamp-jt.iam.gserviceaccount.com"} 
                            # enter your own credentials above
)
credentials_block.save("de-zoomcamp-gcp-creds", overwrite=True)


bucket_block = GcsBucket(
    gcp_credentials=GcpCredentials.load("de-zoomcamp-gcp-creds"),
    bucket="de_zoomcamp_taxi_trips"  # insert your own GCS bucket name
)

bucket_block.save("de-zoomcamp-gcs", overwrite=True)