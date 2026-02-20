from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

def collect_page_text(url):
    print("Initialisation de Selenium...")
    service = ChromeService(executable_path=r'C:\Users\aigen\Desktop\Informatique\botparser\chromedriver_win32\chromedriver.exe')
    driver = webdriver.Chrome(service=service)

    print(f"Visiter l'URL: {url}")
    driver.get(url)

    print("Attendre que la page se charge...")
    WebDriverWait(driver, 2000).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))

    print("Récupérer le contenu de la page...")
    page_content = driver.page_source

    print("Fermer le navigateur...")
    driver.quit()

    print("Utiliser BeautifulSoup pour analyser le contenu HTML...")
    soup = BeautifulSoup(page_content, 'html.parser')

    print("Extraire le texte de toutes les balises <p>...")
    paragraphs = soup.find_all('p')
    text_list = [paragraph.get_text() for paragraph in paragraphs]

    return text_list

# URL de la page que vous souhaitez parcourir
url = "https://www.youtube.com/watch?v=PlzV4aJ7iMI"

print("Appeler la fonction pour récupérer le texte de la page...")
result = collect_page_text(url)

print("Afficher le résultat...")
for text in result:
    print(text)
