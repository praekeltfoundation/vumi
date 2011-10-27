import random
import json
import yaml
from datetime import datetime

from twisted.internet.defer import inlineCallbacks
from twisted.python import log

from vumi.message import Message, TransportUserMessage
from vumi.service import Worker
from vumi.tests.utils import FakeRedis
from vumi.transports.httprpc.vodacom_messaging import VodacomMessagingResponse
from vumi.database.base import (setup_db, get_db, close_db, UglyModel)


TRANSLATIONS = '''
-
    English: "Thnx 4 taking the Quiz.  U have won R12 airtime! We will send U your airtime voucher. For more info about HIV/AIDS pls phone Aids Helpline 0800012322"
    Zulu: "Siyabonga ngokuphendula ngeHIV. Uwinile! Uzothola i-SMS ne-airtime voucher. Ukuthola okwengeziwe ngeHIV/AIDS shayela i-Aids Helpline 0800012322"
    Afrikaans: "Dankie vir jou deelname aan die vasvra! Jy het R12 lugtyd gewen! Jou lugtyd koepon is oppad! Vir meer inligting oor MIV/Vigs, bel die Vigs-hulplyn 0800012322"
    Sotho: "Rea o leboha ka ho nka karolo ho HIV Quiz. O mohlodi! SMS e tla romelwa le voutjhara ya moya. Lesedi le leng ka HIV/AIDS, letsetsa Aids Helpline 0800012322"
-
    English: "Thnx 4 taking the HIV Quiz.  Unfortunately U are not a lucky winner. For more info about HIV/AIDS, phone Aids Helpline 0800012322"
    Zulu: "Siyabonga ngokuphendula imibuzo ngeHIV. Ngeshwa awuphumelelanga. Ukuthola okwengeziwe ngeHIV/AIDS, shayela Aids Helpline 0800012322"
    Afrikaans: "Dankie vir jou deelname aan die vasvra! Jy is ongelukkig nie 'n gelukkige  wenner nie. Vir meer inligting oor MIV/Vigs, bel Vigs Hulplyn 0800012322"
    Sotho: "Rea o leboha ka ho nka karolo ho HIV Quiz. HA O mohlodi, re tshepa hore o ithutile! Lesedi le leng ka HIV/AIDS, letsetsa Aids Helpline 0800012322"
-
    English: "Continue to win!"
    Zulu: "Qhubeka ukuze uwine!"
    Afrikaans: "Gaan voort om te wen!"
    Sotho: "E ya potsong e latelang"
-
    English: "Exit the quiz"
    Zulu: "Phuma kwi-quiz"
    Afrikaans: "Stop die vasvra"
    Sotho: "E tswa ho quiz"
-
    English: "Thnx for taking the quiz.  We'll let U know if U R a lucky winner within 48 hours."
    Zulu: "Siyabonga ngokuphendula. Sizokwazisa uma uwinile emahoreni angu-48."
    Afrikaans: "Dankie dat jy aan die vasvra deelgeneem het. Ons sal jou binne 48 uur laat weet indien jy 'n wenner is."
    Sotho: "Rea o leboha ka quiz. Re tla o tsebisa haeba o le mohlodi dihoreng tse 48"
-
    English: "Thanks, you have now completed all the HIV/AIDS quiz questions. We'll let U know if U R a lucky winner within 48 hours."
    Zulu:
    Afrikaans:
    Sotho:
-
    English: "Thnx 4 taking the Quiz! Answer easy questions and be 1 of 5000 lucky winners.  Pick your language:"
    Zulu: "Thnx 4 taking the Quiz! Answer easy questions and be 1 of 5000 lucky winners.  Pick your language:"
    Afrikaans: "Thnx 4 taking the Quiz! Answer easy questions and be 1 of 5000 lucky winners.  Pick your language:"
    Sotho: "Thnx 4 taking the Quiz! Answer easy questions and be 1 of 5000 lucky winners.  Pick your language:"
-
    English: "English"
    Zulu: "English"
    Afrikaans: "English"
    Sotho: "English"
-
    English: "Zulu"
    Zulu: "Zulu"
    Afrikaans: "Zulu"
    Sotho: "Zulu"
-
    English: "Afrikaans"
    Zulu: "Afrikaans"
    Afrikaans: "Afrikaans"
    Sotho: "Afrikaans"
-
    English: "Sotho"
    Zulu: "Sotho"
    Afrikaans: "Sotho"
    Sotho: "Sotho"
-
    English: "Please pick your gender"
    Zulu: "Sicela ukhethe ubulili bakho"
    Afrikaans: "Kies asseblief jou geslag"
    Sotho: "Hle, kgetha bong ba hao"
-
    English: "Male"
    Zulu: "Uma ungowesilisa"
    Afrikaans: "Manlik"
    Sotho: "E Motona"
-
    English: "Female"
    Zulu: "Uma ungowesifazane"
    Afrikaans: "Vroulik"
    Sotho: "E Motshehadi"
-
    English: "Please pick your province"
    Zulu: "Khetha isifundazwe sakho"
    Afrikaans: "Kies asseblief jou provinsie"
    Sotho: "Hle, kgetha provinse"
-
    English: "Gauteng"
    Zulu: "Gauteng"
    Afrikaans: "Gauteng"
    Sotho: "Gauteng"
-
    English: "Kwazulu Natal"
    Zulu: "Kwazulu Natal"
    Afrikaans: "Kwazulu Natal"
    Sotho: "Kwazulu Natal"
-
    English: "Limpopo"
    Zulu: "Limpopo"
    Afrikaans: "Limpopo"
    Sotho: "Limpopo"
-
    English: "E Cape"
    Zulu: "Mpumakoloni"
    Afrikaans: "Oos-Kaap"
    Sotho: "Eastern Cape"
-
    English: "Mpumalanga"
    Zulu: "Mpumalanga"
    Afrikaans: "Mpumalanga"
    Sotho: "Mpumalanga"
-
    English: "N Cape"
    Zulu: "Nyakatho Kapa"
    Afrikaans: "Noord-Kaap"
    Sotho: "Northern Cape"
-
    English: "W Cape"
    Zulu: "Ntshonakoloni"
    Afrikaans: "Wes-Kaap"
    Sotho: "Western Cape"
-
    English: "Free State"
    Zulu: "Free State"
    Afrikaans: "Vrystaat"
    Sotho: "Free State"
-
    English: "North West"
    Zulu: "North West"
    Afrikaans: "Noord-Wes"
    Sotho: "North West"
-
    English: "Please pick your age group"
    Zulu: "Khetha ubudala bakho"
    Afrikaans: "Kies asseblief jou ouderdomsgroep"
    Sotho: "Kgetha dilemo tsa hao"
-
    English: "younger than 20"
    Zulu: "Uma ungaphansi kweminyaka engu- 20"
    Afrikaans: "Jonger as 20"
    Sotho: "Ka tlase ho 20"
-
    English: "between 20-29"
    Zulu: "Phakathi kuka-20-29"
    Afrikaans: "Tussen 20-29"
    Sotho: "Pakeng tsa 20-29"
-
    English: "between 30-39"
    Zulu: "Phakathi kuka-30-39"
    Afrikaans: "Tussen 30-39"
    Sotho: "Pakeng tsa 30 - 39"
-
    English: "40 or older"
    Zulu: "Ngaphezu kuka-40"
    Afrikaans: "40 en ouer"
    Sotho: "40 kapa ho feta"
-
    English: "Can U get HIV/AIDS by having sex without a condom?"
    Zulu: "Ungaba sengozini ye-HIV/AIDS uma uya ocansini ungafakanga ikhondomu?"
    Afrikaans: "Kan jy MIV/Vigs kry deur seks sonder 'n kondoom te he?"
    Sotho: "Thobalano ntle ho khondomo e kotsi bakeng sa HIV/AIDS?"
-
    English: "Yes"
    Zulu: "Yebo"
    Afrikaans: "Ja"
    Sotho: "E"
-
    English: "Correct, U can get HIV/AIDS by having sex without a condom."
    Zulu: "Uqinisile, ungangenwa yiHIV ngocansi ngaphandle kwekhondomu."
    Afrikaans: "Jy is reg. Jy kan MIV/Vigs kry deur seks sonder 'n kondoom te he."
    Sotho: "E, thobalano ntle ho khondomo ke kotsi ya HIV."
-
    English: "No"
    Zulu: "Cha"
    Afrikaans: "Nee"
    Sotho: "Tjhe"
-
    English: "Please note: U can get HIV/AIDS by having sex without a condom."
    Zulu: "Sicela uqaphele: Ukuya ocansini ngaphandle kwe khondomu kukubeka engozini yokungenwa igciwane le HIV."
    Afrikaans: "Onthou: Jy kan MIV/Vigs kry deur seks sonder 'n kondoom te he."
    Sotho: "Lemoha: Thobalano ntle ho khonodomo ke kotsi ya HIV."
-
    English: "Can traditional medicine cure HIV/AIDS?"
    Zulu: "Amakhambi endabuko angayelapha iHIV/AIDS?"
    Afrikaans: "Kan tradisionele medisyne MIV/VIGS genees?"
    Sotho: "Na moriana wa setso o fodisa HIV/AIDS?"
-
    English: "Please note: Traditional medicine cannot cure HIV/AIDS. There is no cure for HIV/AIDS."
    Zulu: "Qaphela, Amakhambi endabuko awayelaphi i-HIV/AIDS. Alikh' ikhambi leHIV/AIDS."
    Afrikaans: "Onthou: tradisionele medisyne kan nie MIV/Vigs genees nie. Daar is geen kuur 140 vir MIV/Vigs nie."
    Sotho: "Lemoha: Moriana wa setso ha se pheko ya HIV/AIDS. Ha ho na pheko ya HIV/AIDS."
-
    English: "Correct, traditional medicine can not cure HIV/AIDS.  There is no cure for HIV/AIDS."
    Zulu: "Ushaye khona, Amakhambi endabuko angeke ayelaphe i-HIV/AIDS. Alikho ikhambi leHIV/AIDS."
    Afrikaans: "Jy is reg: tradisionele medisyne kan nie MIV/Vigs genees nie. Daar is geen kuur vir MIV en Vigs nie."
    Sotho: "E, Moriana wa setso ha se pheko ya HIV/AIDS. Ha ho na pheko ya HIV/AIDS."
-
    English: "Is an HIV test at any government clinic free of charge?"
    Zulu: "Kumahhala yini ukuhlolelwa iHIV emtholampilo kahulumeni?"
    Afrikaans: "Is 'n MIV-toets by enige regeringskliniek gratis?"
    Sotho: "Na teko ya HIV tliniking ke mahala?"
-
    English: "Correct, an HV test at any government clinic is free of charge."
    Zulu: "Uqinisile, ukuhlolelwa iHIV emtholampilo kahulumeni kumahhala."
    Afrikaans: "Jy is reg: 'n MIV-toets by enige regeringskliniek is gratis."
    Sotho: "E, Teko ya HIV ke mahala tliniking"
-
    English: "Please Note: an HIV test at any government clinic is free of charge."
    Zulu: "Qaphela: ukuhlolelwa iHIV emtholampilo kahulumeni kumahhala."
    Afrikaans: "Onthou: 'n MIV-toets by enige regeringskliniek is gratis."
    Sotho: "Lemoha: Teko ya HIV ke mahala tliniking"
-
    English: "Is it possible for a person newly infected with HIV, to test HIV negative?"
    Zulu: "Ingaba kuyenzeka ukuthi imiphumela ye HIV/AIDS yomuntu oqeda kuthola igciwane ithi akanalo?"
    Afrikaans: "Is dit moontlik vir 'n persoon, wat nuut-geinfekteer is met MIV, om MIV negatief te toets?"
    Sotho: "Ekaba ho a etsahala hore motho ya qetang hokenwa ke HIV/AIDS, diteko tsa hae dire ha ana yona?"
-
    English: "Correct, a newly-infected HIV positive person can test HIV-negative, if they are tested for HIV in the 'window period'"
    Zulu: "Uqinisile, umuntu osanda kuthola i-HIV angase ahlolwe bese imiphumela ithi akanayo i-HIV. Lokhu kubangelwa ukuthi usuke eseku-'window period'"
    Afrikaans: "Korrek, dit is moontlik vir iemand wat nuut-geinfekteer is met MIV, om MIV negatief te toets. Dit is as gevolg van die venster tydperk."
    Sotho: "O nepile, motho wa tshwaetso ya HIV la pele a ka hloka HIV morao. Hona ke hobane a bile le diteko 'nakong ya kemelo'"
-
    English: "Please Note: a newly-infected HIV positive person can test HIV-negative, if they are tested for HIV in the 'window period'"
    Zulu: "Qaphela: umuntu osanda kuthola i-HIV angase ahlolwe bese imiphumela ithi akanayo i-HIV. Lokhu kubangelwa ukuthi usuke eseku-'window period'"
    Afrikaans: "Onthou, dit is moontlik vir iemand wat nuut-geinfekteer is met MIV, om MIV negatief te toets. Dit is as gevolg van die venster tydperk."
    Sotho: "Lemoha: motho wa tshwaetso ya HIV la pele a ka hloka HIV morao. Hona ke hobane a bile le diteko 'nakong ya kemelo'"
-
    English: "Can HIV be transmitted by sweat?"
    Zulu: "I-HIV ingathelelwana yini ngomjuluko?"
    Afrikaans: "Kan MIV deur iemand se sweet oorgedra word?"
    Sotho: "Na HIV e ka fetiswa ka mofufutso?"
-
    English: "Please note, HIV can not be transmitted by sweat."
    Zulu: "Qaphela, i-HIV ngeke idluliselwe ngomjuluko."
    Afrikaans: "Onthou, MIV kan nie deur iemand se sweet oorgedra word nie"
    Sotho: "Lemoha: HIV e ke ke ya fetiswa ka mofufutso"
-
    English: "Correct, HIV can not be transmitted by sweat."
    Zulu: "Uqinisile, i-HIV ngeke idluliselwe ngomjuluko."
    Afrikaans: "Korrek, MIV kan nie deur iemand se sweet oorgedra word nie"
    Sotho: "O nepile, HIV e ke ke ya fetiswa ka mofufutso"
-
    English: "Is there a herbal medication that can cure HIV/AIDS?"
    Zulu: "Ingabe ukhona umuthi wesintu ongelapha i-HIV/AIDS?"
    Afrikaans: "Is daar kruie-medisyne wat MIV/Vigs kan genees?"
    Sotho: "Na ho na le moriana wa ditlamatlama o ka phekolang HIV/AIDS?"
-
    English: "Please note, there are no herbal medications that can cure HIV/AIDS.  There is no cure for HIV/AIDS."
    Zulu: "Qaphela, awukho umuthi wesintu ongelapha i-HIV/AIDS. Alikho ikhambi le-HIV/AIDS."
    Afrikaans: "Onthou, daar is geen kruie-medisyne wat MIV/Vigs kan genees nie. Daar is geen kuur vir MIV/Vigs nie."
    Sotho: "Lemoha: Ha ho na le meriana ya ditlamatlama e ka phekolang HIV/AIDS? Ha ho na pheko bakeng sa HIV/AIDS."
-
    English: "Correct, there are no herbal medications that can cure HIV/AIDS.  There is no cure for HIV/AIDS."
    Zulu: "Uqinisile, awukho umuthi wesintu ongelapha i-HIV/AIDS. Alikho ikhambi le-HIV/AIDS."
    Afrikaans: "Korrek, daar is geen kruie-medisyne wat MIV/Vigs kan genees nie. Daar is geen kuur vir MIV/Vigs nie."
    Sotho: "O nepile, Na ho na le moriana wa ditlamatlama o ka phekolang HIV/AIDS? Ha ho na pheko bakeng sa HIV/AIDS."
-
    English: "Does a CD4-count tell the strength of a person's immune system?"
    Zulu: "Ingabe i-CD4-count iveza ukuqina kwamasosha omzimba omuntu?"
    Afrikaans: "Dui 'n CD4-telling aan hoe sterk of hoe swak iemand se immuunstelsel is?"
    Sotho: "Na CD4-count e ka bolela matla a masole a mmele wa motho?"
-
    English: "Correct, a CD4-count does tell the strength of a person's immune system."
    Zulu: "Uqinisile, i-CD4-count iveza ukuqina kwamasosha omzimba omuntu."
    Afrikaans: "Korrek, 'n CD4-telling dui aan hoe sterk of hoe swak iemand se immuunstelsel is"
    Sotho: "O nepile, ehlile CD4-count e bolela matla a masole a mmele wa motho"
-
    English: "Please note, a CD4-count does tell the strength of a person's immune system."
    Zulu: "Qaphela, i-CD4-count iveza ukuqina kwamasosha omzimba omuntu."
    Afrikaans: "Onthou, 'n CD4-telling dui aan hoe sterk of hoe swak iemand se immuunstelsel is."
    Sotho: "Lemoha: ehlile, CD4-count e bolela matla a masole a mmele wa motho"
-
    English: "Can HIV be transmitted through a mother's breast milk?"
    Zulu: "Ingabe i-HIV ingadluliselwa ngobisi lwebele likamama? "
    Afrikaans: "Kan MIV oorgedra word deur 'n ma se borsmelk? "
    Sotho: "Na HIV e ka fetiswa ka lebese la mme la letswele. "
-
    English: "Correct, HIV can be transmitted via a mother's breast milk."
    Zulu: "Uqinisile, i-HIV ingadluliselwa ngobisi lwebele likamama."
    Afrikaans: "Korrek, MIV kan oorgedra word deur 'n ma se borsmelk."
    Sotho: "O nepile, HIV e ka fetiswa ka lebese la mme la letswele."
-
    English: "Please note, HIV can be transmitted via a mother's breast milk."
    Zulu: "Qaphela, i-HIV ingadluliselwa ngobisi lwebele likamama."
    Afrikaans: "Onthou, MIV kan oorgedra word deur 'n ma se borsmelk."
    Sotho: "Lemoha, HIV e ka fetiswa ka lebese la mme la letswele."
-
    English: "Is it possible for an HIV positive woman to deliver an HIV negative baby?"
    Zulu: "Kungenzeka yini ukuba owesifazane one-HIV abelethe umntwana ongenayo i-HIV?"
    Afrikaans: "Is dit moontlik vir 'n MIV-positiewe ma, om geboorte te skenk aan 'n MIV-negatiewe baba?"
    Sotho: "Na ho a kgoneha hore mme ya nang le tshwaetso ya HIV a pepe lesea le senang tshwaetso ya HIV?"
-
    English: "Correct, with treatment and planning, it is possible for an HIV positive mother to deliver an HIV-negative baby."
    Zulu: "Uqinisile, ngokwelashwa nangokuhlela, kungenzeka ukuba umama one-HIV athole umntwana ongenayo i-HIV."
    Afrikaans: "Korrek, met medikasie en beplanning is dit moontlik vir 'n MIV-positiewe ma om geboorte te skenk aan 'n MIV-negatiewe baba."
    Sotho: "O nepile, ka kalafo le tlhophiso mme ya nang le tshwaetso ya HIV a kgona ho pepa lesea le senang tshwaetso ya HIV."
-
    English: "Please note, with treatment and planning, it is possible for an HIV positive mother to deliver an HIV-negative baby."
    Zulu: "Qaphela, ngokwelashwa nangokuhlela, kungenzeka ukuba umama one-HIV athole umntwana ongenayo i-HIV."
    Afrikaans: "Onthou, met medikasie en beplanning is dit moontlik vir 'n MIV-positiewe ma om geboorte te skenk aan 'n MIV-negatiewe baba."
    Sotho: "Lemoha, ka kalafo le tlhophiso mme ya nang le tshwaetso ya HIV a kgona ho pepa lesea le senang tshwaetso ya HIV."
-
    English: "Do you immediately have to start ARVs when you test HIV positive?"
    Zulu: "Ingabe kudingeka uqale ama-ARV ngokushesha lapho uhlolwa uthola ukuthi une-HIV?"
    Afrikaans: "Moet 'n mens dadelik met anti-tetroviale middels begin as jy MIV-positief toets?"
    Sotho: "Na o tlameha ho qala hanghang ka diARV ha diteko di re o na le HIV?"
-
    English: "Please note, once testing positive for HIV, you first have to get your CD4 test done, to determine if you qualify for ARVs."
    Zulu: "Qaphela, uma usuhloliwe wathola ukuthi une-HIV, kufanele uqale uhlolwe i-CD4-count, ukuze kutholakale ukuthi uyafaneleka yini ukuthola ama-ARV."
    Afrikaans: "Onthou, nadat 'n mens positief toets, moet 'n CD4-telling eers geneem word om te bepaal of mens vir Anti-retroviale middels gekwalifiseer"
    Sotho: "Lemoha, ha diteko di re o na le HIV, etsa pele diteko tsa hao tsa CD4, ho fumana hore na o loketse ho fumana diARV."
-
    English: "Correct, once testing positive for HIV, you first have to get your CD4 test done, to determine if you qualify for ARVs."
    Zulu: "Uqinisile, uma usuhloliwe wathola ukuthi une-HIV, kufanele uqale uhlolwe i-CD4-count, ukuze kutholakale ukuthi uyafaneleka yini ukuthola ama-ARV."
    Afrikaans: "Korrek, nadat 'n mens positief toets, moet 'n CD4-telling eers geneem word om te bepaal of mens vir Anti-retroviale middels"
    Sotho: "O nepile, ha diteko di re o na le HIV, etsa pele diteko tsa hao tsa CD4, ho fumana hore na o loketse ho fumana diARV."

'''

QUIZ = '''
winner:
    headertext: "Thnx 4 taking the Quiz.  U have won R12 airtime! We will send U your airtime voucher. For more info about HIV/AIDS pls phone Aids Helpline 0800012322"

nonwinner:
    headertext: "Thnx 4 taking the HIV Quiz.  Unfortunately U are not a lucky winner. For more info about HIV/AIDS, phone Aids Helpline 0800012322"

continue:
    options:
        1:
            text: "Continue to win!"
        2:
            text: "Exit the quiz"

early_exit:
    headertext: "Thanks, you have completed this section of the quiz. Dial *120*112233# again, to try more questions and stand another chance to win"

exit:
    headertext: "Thnx for taking the quiz.  We'll let U know if U R a lucky winner within 48 hours."

completed:
    headertext: "Thanks, you have now completed all the HIV/AIDS quiz questions. We'll let U know if U R a lucky winner within 48 hours."

demographic1:
    headertext: "Thnx 4 taking the Quiz! Answer easy questions and be 1 of 5000 lucky winners.  Pick your language:"
    options:
        1:
            text: "English"
        2:
            text: "Zulu"
        3:
            text: "Afrikaans"
        4:
            text: "Sotho"

demographic2:
    headertext: "Please pick your gender"
    options:
        1:
            text: "Male"
        2:
            text: "Female"

demographic3:
    headertext: "Please pick your province"
    options:
        1:
            text: "Gauteng"
        2:
            text: "Kwazulu Natal"
        3:
            text: "Limpopo"
        4:
            text: "E Cape"
        5:
            text: "Mpumalanga"
        6:
            text: "N Cape"
        7:
            text: "W Cape"
        8:
            text: "Free State"
        9:
            text: "North West"

demographic4:
    headertext: "Please pick your age group"
    options:
        1:
            text: "younger than 20"
        2:
            text: "between 20-29"
        3:
            text: "between 30-39"
        4:
            text: "40 or older"

question1:
    headertext: "Can U get HIV/AIDS by having sex without a condom?"
    correct_answer: 1
    options:
        1:
            text: "Yes"
            reply: "Correct, U can get HIV/AIDS by having sex without a condom."
        2:
            text: "No"
            reply: "Please note: U can get HIV/AIDS by having sex without a condom."

question2:
    headertext: "Can traditional medicine cure HIV/AIDS?"
    correct_answer: 2
    options:
        1:
            text: "Yes"
            reply: "Please note: Traditional medicine cannot cure HIV/AIDS. There is no cure for HIV/AIDS."
        2:
            text: "No"
            reply: "Correct, traditional medicine can not cure HIV/AIDS.  There is no cure for HIV/AIDS."

question3:
    headertext: "Is an HIV test at any government clinic free of charge?"
    correct_answer: 1
    options:
        1:
            text: "Yes"
            reply: "Correct, an HV test at any government clinic is free of charge."
        2:
            text: "No"
            reply: "Please Note: an HIV test at any government clinic is free of charge."

question4:
    headertext: "Is it possible for a person newly infected with HIV, to test HIV negative?"
    correct_answer: 1
    options:
        1:
            text: "Yes"
            reply: "Correct, a newly-infected HIV positive person can test HIV-negative, if they are tested for HIV in the 'window period'"
        2:
            text: "No"
            reply: "Please Note: a newly-infected HIV positive person can test HIV-negative, if they are tested for HIV in the 'window period'"

question5:
    headertext: "Can HIV be transmitted by sweat?"
    correct_answer: 2
    options:
        1:
            text: "Yes"
            reply: "Please note, HIV can not be transmitted by sweat."
        2:
            text: "No"
            reply: "Correct, HIV can not be transmitted by sweat."

question6:
    headertext: "Is there a herbal medication that can cure HIV/AIDS?"
    correct_answer: 2
    options:
        1:
            text: "Yes"
            reply: "Please note, there are no herbal medications that can cure HIV/AIDS.  There is no cure for HIV/AIDS."
        2:
            text: "No"
            reply: "Correct, there are no herbal medications that can cure HIV/AIDS.  There is no cure for HIV/AIDS."

question7:
    headertext: "Does a CD4-count tell the strength of a person's immune system?"
    correct_answer: 1
    options:
        1:
            text: "Yes"
            reply: "Correct, a CD4-count does tell the strength of a person's immune system."
        2:
            text: "No"
            reply: "Please note, a CD4-count does tell the strength of a person's immune system."

question8:
    headertext: "Can HIV be transmitted through a mother's breast milk?"
    correct_answer: 1
    options:
        1:
            text: "Yes"
            reply: "Correct, HIV can be transmitted via a mother's breast milk."
        2:
            text: "No"
            reply: "Please note, HIV can be transmitted via a mother's breast milk."

question9:
    headertext: "Is it possible for an HIV positive woman to deliver an HIV negative baby?"
    correct_answer: 1
    options:
        1:
            text: "Yes"
            reply: "Correct, with treatment and planning, it is possible for an HIV positive mother to deliver an HIV-negative baby."
        2:
            text: "No"
            reply: "Please note, with treatment and planning, it is possible for an HIV positive mother to deliver an HIV-negative baby."

question10:
    headertext: "Do you immediately have to start ARVs when you test HIV positive?"
    correct_answer: 2
    options:
        1:
            text: "Yes"
            reply: "Please note, once testing positive for HIV, you first have to get your CD4 test done, to determine if you qualify for ARVs."
        2:
            text: "No"
            reply: "Correct, once testing positive for HIV, you first have to get your CD4 test done, to determine if you qualify for ARVs."

'''


class IkhweziModel(UglyModel):
    TABLE_NAME = 'ikhwezi_quiz'
    fields = (
        ('id', 'SERIAL PRIMARY KEY'),
        ('msisdn', 'varchar UNIQUE NOT NULL'),
        ('provider', 'varchar'),
        ('sessions', 'integer'),
        ('msisdn_timestamp', 'timestamp'),
        ('question1', 'integer'),
        ('question2', 'integer'),
        ('question3', 'integer'),
        ('question4', 'integer'),
        ('question5', 'integer'),
        ('question6', 'integer'),
        ('question7', 'integer'),
        ('question8', 'integer'),
        ('question9', 'integer'),
        ('question10', 'integer'),
        ('question1_correct', 'integer'),
        ('question2_correct', 'integer'),
        ('question3_correct', 'integer'),
        ('question4_correct', 'integer'),
        ('question5_correct', 'integer'),
        ('question6_correct', 'integer'),
        ('question7_correct', 'integer'),
        ('question8_correct', 'integer'),
        ('question9_correct', 'integer'),
        ('question10_correct', 'integer'),
        ('question1_timestamp', 'timestamp'),
        ('question2_timestamp', 'timestamp'),
        ('question3_timestamp', 'timestamp'),
        ('question4_timestamp', 'timestamp'),
        ('question5_timestamp', 'timestamp'),
        ('question6_timestamp', 'timestamp'),
        ('question7_timestamp', 'timestamp'),
        ('question8_timestamp', 'timestamp'),
        ('question9_timestamp', 'timestamp'),
        ('question10_timestamp', 'timestamp'),
        ('demographic1', 'integer'),
        ('demographic2', 'integer'),
        ('demographic3', 'integer'),
        ('demographic4', 'integer'),
        ('demographic1_timestamp', 'timestamp'),
        ('demographic2_timestamp', 'timestamp'),
        ('demographic3_timestamp', 'timestamp'),
        ('demographic4_timestamp', 'timestamp'),
        ('remaining_questions', 'varchar'),
        ('original_questions', 'varchar'),
        ('winner', 'varchar'),
        ('winner_timestamp', 'timestamp')
        )
    indexes = [
        'provider',
        'sessions',
        'msisdn_timestamp',
        'question1',
        'question2',
        'question3',
        'question4',
        'question5',
        'question6',
        'question7',
        'question8',
        'question9',
        'question10',
        'question1_correct',
        'question2_correct',
        'question3_correct',
        'question4_correct',
        'question5_correct',
        'question6_correct',
        'question7_correct',
        'question8_correct',
        'question9_correct',
        'question10_correct',
        'demographic1',
        'demographic2',
        'demographic3',
        'demographic4',
        'winner'
        ]

    @classmethod
    def get_item(cls, txn, msisdn):
        items = cls.run_select(txn, "WHERE msisdn=%(msisdn)s",
                                   {'msisdn': msisdn})
        if items:
            return cls(txn, *items[0])
        return None

    @classmethod
    def create_item(cls, txn, **params):
        insert_stmt = cls.insert_values_query(**params)
        txn.execute(cls.insert_values_query(**params), params)
        txn.execute("SELECT lastval()")
        return txn.fetchone()[0]

    @classmethod
    def param_format(cls, p):
        try:
            s = json.dumps(p)
        except:
            try:
                s = json.dumps(p.strftime("%Y-%m-%d %H:%M:%S+00"))
            except:
                s = json.dumps(str(p))
        if s.startswith('"'):
            s = "'%s'" % s[1:-1]
        return s

    @classmethod
    def update_query(cls, **kw):
        msisdn = kw.pop('msisdn')
        try:
            kw.pop('id')
        except:
            pass
        valuespecs = ", ".join(["%s = %s" % (
            k, cls.param_format(v)) for k, v in kw.items()])
        query = "UPDATE %s SET %s WHERE msisdn = '%s'" % (
            cls.get_table_name(), valuespecs, msisdn)
        return query

    @classmethod
    def update_item(cls, txn, **params):
        txn.execute(cls.update_query(**params))
        return None

class IkhweziQuiz():

    def __init__(self, config, quiz, translations, db):
        self.config = config
        self.quiz = quiz
        self.translations = translations
        self.db = db

    def respond(self, msisdn, session_event, provider, request,
            response_callback):
        self.msisdn = str(msisdn)
        self.session_event = session_event
        self.provider = provider
        self.request = request
        self.response_callback = response_callback
        d = self.retrieve_entrant()
        return d

    def existing_respond(self, *args, **kwargs):
        def send_response(*args, **kwargs):
            self.response_callback(self.response)
        self.response = self.formulate_response()
        d = self.ds_set()
        d.addCallback(send_response)
        return d

    def ri(self, *args, **kw):
        return self.db.runInteraction(*args, **kw)

    def ds_set(self):
        self.data['remaining_questions'] = json.dumps(self.remaining)
        def _txn(txn):
            IkhweziModel.update_item(txn, **self.data)
        d = self.ri(_txn)
        return d

    def ds_get(self):
        def _txn(txn):
            item = IkhweziModel.get_item(txn, self.msisdn)
            return item
        d = self.ri(_txn)
        return d

    def ds_new(self):
        def _txn(txn):
            item = IkhweziModel.create_item(txn, **self.data)
            return item
        d = self.ri(_txn)
        return d

    def lang(self, lang):
        langs = {
                "English": "1",
                "Zulu": "2",
                "Afrikaans": "3",
                "Sotho": "4",
                "1": "English",
                "2": "Zulu",
                "3": "Afrikaans",
                "4": "Sotho"
                }
        return langs.get(str(lang))

    def _(self, string):
        trans = self.translations.get(self.language)
        if trans == None:
            return string
        else:
            newstring = trans.get(string)
            if newstring == None:
                return string
            else:
                return newstring

    def retrieve_entrant(self):
        def handle_item(item):
            if item is None:
                d = self.new_entrant(self.msisdn, self.provider)
                return d
            else:
                self.data = {}
                for t in item.fields:
                    self.data[t[0]] = getattr(item, t[0])
                self.language = self.lang(self.data['demographic1'] or 1)
                self.remaining = json.loads(self.data['remaining_questions'])
                if self.session_event == 'new':
                    self.data['sessions'] += 1
                d = self.existing_respond()
                return d
        d = self.ds_get()
        d.addCallback(handle_item)
        return d

    def random_remaining_questions(self):
        random.seed()
        rq = []
        rq.append(['demographic1'])
        demo = [
                'demographic2',
                'demographic3',
                'demographic4'
                ]
        random.shuffle(demo)
        for d in demo:
            rq.append([d])
        ques = [
                'question1',
                'question2',
                'question3',
                'question4',
                'question5',
                'question6',
                'question7',
                'question8',
                'question9',
                'question10']
        random.shuffle(ques)
        while len(ques):
            for i in range(4):
                try:
                    rq[i].append(ques.pop())
                    rq[i].append('continue')
                except:
                    pass
        return rq

    def new_entrant(self, msisdn, provider=None):
        self.remaining = self.random_remaining_questions()
        self.language = "English"
        self.data = {
                'msisdn': str(msisdn),
                'provider': provider,
                'sessions': 1,
                'msisdn_timestamp': datetime.utcnow().strftime(
                    "%Y-%m-%d %H:%M:%S+00"),
                'question1': None,
                'question2': None,
                'question3': None,
                'question4': None,
                'question5': None,
                'question6': None,
                'question7': None,
                'question8': None,
                'question9': None,
                'question10': None,
                'question1_correct': 0,
                'question2_correct': 0,
                'question3_correct': 0,
                'question4_correct': 0,
                'question5_correct': 0,
                'question6_correct': 0,
                'question7_correct': 0,
                'question8_correct': 0,
                'question9_correct': 0,
                'question10_correct': 0,
                'question1_timestamp': None,
                'question2_timestamp': None,
                'question3_timestamp': None,
                'question4_timestamp': None,
                'question5_timestamp': None,
                'question6_timestamp': None,
                'question7_timestamp': None,
                'question8_timestamp': None,
                'question9_timestamp': None,
                'question10_timestamp': None,
                'demographic1': None,
                'demographic2': None,
                'demographic3': None,
                'demographic4': None,
                'demographic1_timestamp': None,
                'demographic2_timestamp': None,
                'demographic3_timestamp': None,
                'demographic4_timestamp': None,
                'remaining_questions': json.dumps(self.remaining),
                'original_questions': json.dumps(self.remaining),
                'winner': None,
                'winner_timestamp': None}
        def on_new(item):
            self.response_callback(self.formulate_response())
        d = self.ds_new()
        d.addCallback(on_new)
        return d

    def answer_question(self, question_name, answer):
        reply = None
        question = self.quiz.get(question_name)
        if question_name in self.data.keys() \
                and answer in question['options'].keys():
            reply = question['options'][answer].get('reply')
            self.data[question_name] = answer
            self.data[question_name + '_timestamp'] = \
                    datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S+00")
            correct_answer = question.get('correct_answer')
            if correct_answer:
                if correct_answer == answer:
                    self.data[question_name + '_correct'] = 1
                else:
                    self.data[question_name + '_correct'] = -1
            self.remaining_list().pop(0)
            self.language = self.lang(self.data['demographic1'] or 1)
        return reply

    def do_continue(self, answer):
        exit = False
        self.remaining_list().pop(0)
        if answer == 2 or len(self.remaining_list()) == 0:
            exit = True
        return exit

    def remaining_list(self):
        r = []
        try:
            r = self.remaining[self.data['sessions']-1]
        except:
            pass
        return r

    def formulate_response(self):
        question_name = None
        if len(self.remaining_list()):
            question_name = self.remaining_list()[0]

        try:
            answer = int(self.request)
        except Exception, e:
            answer = None

        if self.data['sessions'] > 4 or question_name == None:
            # terminate interaction
            question = self.quiz.get('completed')
            headertext = self._(question['headertext'])
            return {"content": headertext,
                    "continue_session": False}

        reply = None
        if question_name != 'continue':
            reply = self.answer_question(question_name, answer)

        exit = False
        if question_name == 'continue':
            exit = self.do_continue(answer)

        if exit:
            # don't continue, show exit
            question = self.quiz.get('exit')
            headertext = self._(question['headertext'])
            if self.data['sessions'] < 4:
                question = self.quiz.get('early_exit')
                headertext = self._(question['headertext'])
            return {"content": headertext,
                    "continue_session": False}

        elif reply:
            # correct answer and offer to continue
            question = self.quiz.get('continue')
            headertext = self._(reply)
            for key, val in question['options'].items():
                headertext += "\n%s. %s" % (key, self._(val['text']))
            return {"content": headertext,
                    "continue_session": True}

        else:
            # ask another question
            question_name = self.remaining_list()[0]
            question = self.quiz.get(question_name)
            headertext = self._(question['headertext'])
            for key, val in question['options'].items():
                headertext += "\n%s. %s" % (key, self._(val['text']))
            return {"content": headertext,
                    "continue_session": True}


class IkhweziQuizWorker(Worker):
    @inlineCallbacks
    def startWorker(self):
        log.msg("Starting IkhweziQuizWorker with config: %s" % (self.config))
        self.publish_key = self.config['publish_key']
        self.consume_key = self.config['consume_key']

        self.setup_db()
        log.msg("DB setup with: %s" % (self.db))

        self.publisher = yield self.publish_to(self.publish_key)
        self.consume(self.consume_key, self.consume_message)

        self.quiz = yaml.load(QUIZ)
        trans = yaml.load(TRANSLATIONS)
        self.translations = {'Zulu': {}, 'Sotho': {}, 'Afrikaans': {}}
        for t in trans:
            self.translations['Zulu'][t['English']] = t['Zulu']
            self.translations['Sotho'][t['English']] = t['Sotho']
            self.translations['Afrikaans'][t['English']] = t['Afrikaans']

    def setup_db(self):
        dbname = 'ikhwezi'
        try:
            get_db(dbname)
            close_db(dbname)
        except:
            pass
        self.db = setup_db(dbname, database=dbname,
                host='localhost',
                user='vumi',
                password='vumi')
        return self.db.runQuery("SELECT 1")

    def consume_message(self, message):
        log.msg("IkhweziQuizWorker consuming on %s: %s" % (
            self.consume_key,
            repr(message.payload)))
        user_m = TransportUserMessage(**message.payload)
        request = user_m.payload['content']
        msisdn = user_m.payload['from_addr']
        session_event = user_m.payload.get('session_event')
        provider = user_m.payload.get('provider')
        def response_callback(resp):
            reply = user_m.reply(resp['content'], resp['continue_session'])
            self.publisher.publish_message(reply)
        ik = IkhweziQuiz(
                self.config,
                self.quiz,
                self.translations,
                self.db)
        ik.respond(
                msisdn,
                session_event,
                provider,
                request,
                response_callback)

    def stopWorker(self):
        log.msg("Stopping the IkhweziQuizWorker")
