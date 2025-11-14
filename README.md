
Welcome to DOX 1.0 Web

Stručný průvodce.
K čemu aplikace slouží

    Správa tabulek v SQLite (soubor db.sqlite3).
    Import dat z CSV/JSON souborů a z webových stránek (včetně stránek OTE).
    Tvorba dotazů nad tabulkami (filtry, výběry, agregace SUM/AVERAGE/COUNT/MAX/MIN) v části ASK.
    Export výsledků do CSV/JSON.
    Základní prohlížení a editace dat v tabulkách, tvorba nových tabulek.

Jak aplikaci spustit

    Nainstalujte závislosti (v adresáři projektu):

cd paradox45web
pip install -r requirements.txt

    Spusťte server:

python manage.py runserver

Otevřete v prohlížeči: http://127.0.0.1:8000/
Přehled menu a hlavních obrazovek

    Domů (tato stránka): základní informace.
    Seznam tabulek: přehled tabulek, otevření detailu, smazání (s potvrzením). Odkaz: View
    Editace tabulky: úprava hodnot, přidání/smazání řádku.
    Tvorba tabulky: ruční definice nové tabulky (jméno, sloupce a typy). Odkaz: Create table
    ASK: stavitel dotazu, filtry a agregace. Odkaz: ASK
    Import: import ze souboru, webu, OTE + mezikrok “preview”. Odkaz: Import

ASK: dotazy a agregace

    Vyberte tabulku.
    U sloupců lze zaškrtnout výběr (SELECT), nastavit filtr (čísla: =, <, >; text: exact/startswith/contains) a zvolit Summary operator (SUM/AVERAGE/COUNT/MAX/MIN).
    Pokud použijete agregace, zbylé vybrané sloupce tvoří GROUP BY.
    Výsledek můžete uložit jako novou tabulku (zadáním názvu) nebo exportovat do CSV/JSON.

Import
1) Soubor (CSV/JSON)

    Nahrajte soubor v části Import.
    CSV: čtení v UTF-8, fallback CP1250; prázdné hodnoty → NULL.
    JSON: podporuje list objektů (se stejnými klíči) i dict-of-lists.
    Převod vytvoří novou tabulku, typy sloupců se odhadnou automaticky.

2) Z webu (libovolná URL)

    Zadejte URL a klikněte “Načíst tabulky”; pokud se tabulky renderují v JavaScriptu, použijte “Načíst (JS)”.
    Zobrazí se seznam nalezených tabulek s náhledem.
    Máte dvě možnosti:
        Připravit: otevře Import preview – můžete přejmenovat sloupce, vynutit typy (INTEGER/REAL/TEXT/DATE/DATETIME) a zvolit formát čísel a datumu; pak “Importovat”.
        Importovat: rychlý import bez preview (automatická detekce typů).

3) OTE (Denní trh)

    Zadejte datum a time resolution (např. PT15M) a klikněte “Načíst OTE”.
    U vybrané tabulky využijte Připravit (preview) nebo Importovat (rychlý import).

Prohlížení a editace tabulek

    Seznam tabulek (View): všechny nesystémové tabulky, možnost otevřít, smazat (s potvrzením).
    Editace: přidání nového řádku, úprava buněk, smazání řádku (ikona koše). Sloupcová struktura se zde nemění.

Tvorba tabulek

    V Create table zadejte název tabulky a definujte sloupce (jméno a SQL typ).
    Tabulku lze poté plnit ručně nebo importem.

Export a log

    V ASK lze výsledky exportovat do CSV/JSON. Název souboru vychází z pole “save name”.
    Stránka Import zobrazuje Log posledních importů (in-memory): čas, zdroj, typ, jméno tabulky, řádky/sloupce.

Tipy a omezení

    DATE/DATETIME se v SQLite ukládají jako ISO TEXT (YYYY-MM-DD resp. YYYY-MM-DD HH:MM:SS).
    Detekce typů je heuristika; pro přesnost využijte preview a typy vynucujte.
    Názvy tabulek/sloupců se sanitizují, duplicitám se přidávají suffixy (_1, _2...).
    Velké tabulky/HTML parsing může trvat déle, u JS stránek použijte “Načíst (JS)”.
    Cache (načtené tabulky) a import log jsou v paměti – po restartu se vyprázdní.

Řešení potíží

    No module named 'bs4': nainstalujte beautifulsoup4 a lxml (jsou v requirements).
    Načíst (JS) nefunguje: doinstalujte requests-html.
    “Nenalezeny žádné tabulky”, ale ve zdrojáku jsou: zkuste “Načíst (JS)” nebo přímý iframe odkaz.
    Export nerespektuje jméno: vyplňte “save name” bez přípony; přípona se doplní dle formátu.
    Po importu tabulka neviditelná: zkontrolujte přidaný suffix unikátního jména, případně otevřete Seznam tabulek.

Příklady workflow
SUM platů podle pozice

    ASK → vyberte tabulku (např. main_employee).
    Zaškrtněte pozice (bude v GROUP BY), u plat nastavte Summary operator = SUM.
    Odešlete; výsledek můžete uložit jako novou tabulku nebo exportovat.

Import HTML tabulky s preview

    Import → zadejte URL → “Načíst tabulky”.
    Klikněte “Připravit”; přejmenujte sloupce, nastavte typy/formáty → zadejte cílový název → “Importovat”.

