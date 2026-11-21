# Exemples XML de la collection Postman

Les identifiants R01 à R77 suivent l’ordre de la collection. L’export du 21 novembre 2026 contient 81 réponses enregistrées, dont 80 corps ou membres XML analysables, ainsi qu’un corps XML de requête distinct. Chaque entrée renvoie à la fiche et à la [table des racines, espaces de noms et XSD](corpus-xml.md#racines-et-schémas). [Collection Postman](https://documenter.getpostman.com/view/7009892/2s93JtP3F6) · [Correspondances officielles par jeu de données](https://transparencyplatform.zendesk.com/hc/en-us/articles/17260622859412-Transparency-Platform-Help-page)

Un nom de membre suivi de « Nom seulement » apparaît dans l’exemple sans XML visible. Les compteurs décrivent le contenu visible de l’exemple. Les cas de test du transport doivent recréer un ZIP à partir des membres XML disponibles. Le corps binaire altéré de R39 a un exemple textuel de remplacement dans la même fiche.

### R01

Market · [12.1.E Implicit and Flow-based Allocations - Congestion Income](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#1a3c851c-6144-49e3-b0d6-45632e94e366)

- **DE(TenneT GER)-CZ (Intraday 25/08/2023)** · HTTP 200
  - [S23](corpus-xml.md#s23) · 1 `TimeSeries`, 1 `Period`, 96 `Point`, A01.
- **AT (Daily flow based 24/08/2023)** · HTTP 200
  - [S23](corpus-xml.md#s23) · 1 `TimeSeries`, 1 `Period`, 24 `Point`, A01.

### R02

Market · [12.1.B Total Nominated Capacity](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#518599da-fa8a-4b3a-9d3d-6ed4c6a23aa3)

- **GB-->BE (21/08/2023)** · HTTP 200
  - [S23](corpus-xml.md#s23) · 3 `TimeSeries`, 3 `Period`, 72 `Point`, A01.

### R03

Market · [11.1 Implicit Allocations - Offered Transfer Capacity](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#b4684245-411f-49bc-9971-9b27e4dbef61)

- **DK1-->DE-LU (01/01/2023)** · HTTP 200
  - [S23](corpus-xml.md#s23) · 1 `TimeSeries`, 1 `Period`, 24 `Point`, A01.
- **FR-->BE (Intraday 01/01/2023)** · HTTP 200
  - [S24](corpus-xml.md#s24) · 1 `TimeSeries`, 1 `Period`, 48 `Point`, A01.

### R04

Market · [12.1.H Transfer Capacities Allocated with Third Countries \[12.1.H\] (explicit)](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#22238b89-7544-45fe-8568-1c0904371933)

- **FI-->RU (Intraday 24/08/2023)** · HTTP 200
  - [S23](corpus-xml.md#s23) · 1 `TimeSeries`, 1 `Period`, 24 `Point`, A01.

### R05

Market · [12.1.C Total Capacity Already Allocated](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#eb96cc46-c0b7-42dd-8e1a-962e84dac9ae)

- **HR-->BA (Daily 25/08/2023)** · HTTP 200
  - [S23](corpus-xml.md#s23) · 1 `TimeSeries`, 1 `Period`, 24 `Point`, A01.

### R06

Market · [11.1.A Explicit Allocations - Offered Transfer Capacity](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#d7613bd7-223d-435b-9970-f7cc449effea)

- **GB-->BE (Day ahead 16/08/2023)** · HTTP 200
  - [S23](corpus-xml.md#s23) · 1 `TimeSeries`, 1 `Period`, 24 `Point`, A01.

### R07

Market · [12.1.A Explicit Allocations - Use of the Transfer Capacity](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#a6fa8748-a0a6-491d-8e46-b21b0e416499)

- **GB-->BE (RC 16/08/2023)** · HTTP 200
  - [S23](corpus-xml.md#s23) · 4 `TimeSeries`, 4 `Period`, 24 `Point`, A01.
- **GB-->BE (AC+price 16/08/2023)** · HTTP 200
  - [S23](corpus-xml.md#s23) · 4 `TimeSeries`, 4 `Period`, 24 `Point`, A01.

### R08

Market · [12.1.A Explicit Allocations - Auction Revenue](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#f5a00346-3a31-41f9-bd00-6d348a94ae44)

- **HR-->BA (Daily 25/08/2023)** · HTTP 200
  - [S23](corpus-xml.md#s23) · 1 `TimeSeries`, 1 `Period`, 24 `Point`, A01.

### R09

Market · [12.1.E Implicit Auction — Net Positions](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#6e47e57a-93b8-40dd-8286-d3f487009c7a)

- **BE (Intraday 23/08/2023)** · HTTP 200
  - [S23](corpus-xml.md#s23) · 1 `TimeSeries`, 1 `Period`, 96 `Point`, A01.

### R10

Market · [11.1.B Flow Based Allocations](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#2bf6517a-3fe8-4877-b6e5-e12a1ea4586d)

- **Nordic (2025 month-ahead)** · HTTP 200
  - Flow_based_Allocations_202412312300-202512312300.xml · [S15](corpus-xml.md#s15) · 1 `TimeSeries`, 1 `Period`, 1 `Point`, 8 `RegisteredResource`, 4 `Constraint_Series`, 4 `Contingency_Series`, A01.
- **CWE (04/02/2014 - test data)** · HTTP 200
  - Flow_based_Allocations_201402032300-201402040000.xml · [S15](corpus-xml.md#s15) · 1 `TimeSeries`, 1 `Period`, 1 `Point`, 2 `RegisteredResource`, 1 `Constraint_Series`, 1 `Contingency_Series`, A01.

### R11

Market · [11.1.B Flow Based Allocations Archives](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#1c115bad-7523-4d6b-b752-79f7e96a05e7)

- **11.1.B Flow Based Allocations Archives** · HTTP 200
  - DayAhead_CWE_20181230T2300Z_20181231T2300Z.xml · [S16](corpus-xml.md#s16) · 1 `TimeSeries`, 1 `Period`, 24 `Point`, A01.

### R12

Market · [11.1 Continuous Allocations - Offered Transfer Capacity](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#d926cea4-d96a-4ef1-8cfa-9d87203a537f)

- **BE-->NL (16/05/2024)** · HTTP 200
  - [S24](corpus-xml.md#s24) · 1 `TimeSeries`, 1 `Period`, 80 `Point`, A03.

### R13

Market · [12.1.D Energy Prices](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#3b383df0-ada2-49fe-9a50-98b1bb201c6b)

- **AT (28/07/2024)** · HTTP 200
  - [S24](corpus-xml.md#s24) · 1 `TimeSeries`, 1 `Period`, 95 `Point`, A03.

### R14

Load · [6.1.A Actual Total Load - SECURITY_TOKEN in header](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#f6b9b190-2b87-4cd1-9409-0c4f39399881)

- Réutiliser la réponse XML du GET [R16](#r16) et vérifier l’authentification dans l’en-tête.

### R15

Load · [6.1.A Actual Total Load (Post)](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#7ccbc838-4c05-45de-a5c8-9f506746e986)

- Corps de requête · [S28](corpus-xml.md#s28) · 4 `AttributeInstanceComponent`. Le même XML accompagne la requête de l’exemple de réponse enregistré.

- **CZ (01/01/2016-31/12/2016 CET)** · HTTP 200
  - [S18](corpus-xml.md#s18) · 9 `TimeSeries`, 9 `Period`, 8776 `Point`, A01.

### R16

Load · [6.1.A Actual Total Load](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#b223e451-65c9-4e24-a782-2af31b431742)

- **CZ (03/03/2023-05/03/2023 UTC)** · HTTP 200
  - [S18](corpus-xml.md#s18) · 1 `TimeSeries`, 1 `Period`, 72 `Point`, A01.

### R17

Load · [6.1.B Day-ahead Total Load Forecast](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#10c431a9-69a9-4a76-8674-923c2eff48ed)

- **CZ (14/08/2023-16/08/2023 UTC)** · HTTP 200
  - [S18](corpus-xml.md#s18) · 4 `TimeSeries`, 4 `Period`, 96 `Point`, A01.

### R18

Load · [6.1.C Week-ahead Total Load Forecast](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#0e0b45ab-06e2-4ccf-847e-c1fcb2bcba81)

- **CZ (Week 33 2023)** · HTTP 200
  - [S18](corpus-xml.md#s18) · 2 `TimeSeries`, 2 `Period`, 14 `Point`, A01.

### R19

Load · [6.1.D Month-ahead Total Load Forecast](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#93c91c9a-349d-4bbe-aad4-6e9daea3aff5)

- **CZ (July 2023)** · HTTP 200
  - [S18](corpus-xml.md#s18) · 2 `TimeSeries`, 2 `Period`, 10 `Point`, A01.

### R20

Load · [6.1.E Year-ahead Total Load Forecast](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#0af80639-7be2-4c91-b108-df179259e34e)

- **CZ (2023)** · HTTP 200
  - [S18](corpus-xml.md#s18) · 2 `TimeSeries`, 2 `Period`, 104 `Point`, A01.

### R21

Load · [8.1 Year-ahead Forecast Margin](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#5781e4e4-9795-49c3-b6e6-bab3123caa9e)

- **CZ (2023)** · HTTP 200
  - [S18](corpus-xml.md#s18) · 1 `TimeSeries`, 1 `Period`, 1 `Point`, A01.

### R22

Generation · [14.1.A Installed Capacity per Production Type](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#93160892-f305-43d8-80e7-545535250034)

- **BE (2023)** · HTTP 200
  - [S18](corpus-xml.md#s18) · 11 `TimeSeries`, 11 `Period`, 11 `Point`, A01.
- **CH (2023)** · HTTP 200
  - [S18](corpus-xml.md#s18) · 4 `TimeSeries`, 4 `Period`, 4 `Point`, A01.

### R23

Generation · [16.1.D Water Reservoirs and Hydro Storage Plants](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#cb402c39-5ee5-4dbb-a1cd-e681d5a524f8)

- **BG (week 28, 2023)** · HTTP 200
  - [S18](corpus-xml.md#s18) · 1 `TimeSeries`, 1 `Period`, 1 `Point`, A01.

### R24

Generation · [16.1.B&C Actual Generation per Production Type](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#d4383852-1e53-4f98-a028-e0d9ac73d5f5)

- **DE (16/08/2023)** · HTTP 200
  - [S18](corpus-xml.md#s18) · 19 `TimeSeries`, 19 `Period`, 1824 `Point`, A01.

### R25

Generation · [16.1.A Actual Generation per Generation Unit](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#537e921c-b261-46d1-9515-32958e8f520a)

- **BE (Nuclear 16/08/2023)** · HTTP 200
  - [S18](corpus-xml.md#s18) · 6 `TimeSeries`, 6 `Period`, 144 `Point`, A01.

### R26

Generation · [14.1.C Generation Forecast - Day ahead](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#e2e1a56e-2ee1-4b83-b1db-8a3d21cc0ac0)

- **BE (16/08/2023)** · HTTP 200
  - [S18](corpus-xml.md#s18) · 1 `TimeSeries`, 1 `Period`, 24 `Point`, A01.

### R27

Generation · [14.1.D Generation Forecasts for Wind and Solar](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#76044772-c723-4d1e-a4b5-f0af05445256)

- **BE (Day ahead - 16/08/2023)** · HTTP 200
  - [S18](corpus-xml.md#s18) · 3 `TimeSeries`, 3 `Period`, 72 `Point`, A01.
- **BE (Current - 16/08/2023)** · HTTP 200
  - [S18](corpus-xml.md#s18) · 3 `TimeSeries`, 3 `Period`, 72 `Point`, A01.
- **BE (Intraday - 16/08/2023)** · HTTP 200
  - [S18](corpus-xml.md#s18) · 3 `TimeSeries`, 3 `Period`, 72 `Point`, A01.

### R28

Generation · [14.1.B Installed Capacity Per Production Unit](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#f5a99f77-1a34-4497-869e-45d8fe2b14da)

- **BE (01/08/2023)** · HTTP 200
  - [S18](corpus-xml.md#s18) · 33 `TimeSeries`, 33 `Period`, 33 `Point`, A01.

### R29

Transmission · [12.1.G Cross-Border Physical Flows](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#cf2e0dec-50ef-45e0-8da9-f13eb4454733)

- **DE(Amprion)-->BE (24/08/2023)** · HTTP 200
  - [S23](corpus-xml.md#s23) · 1 `TimeSeries`, 1 `Period`, 96 `Point`, A01.

### R30

Transmission · [11.1.A Forecasted Transfer Capacities](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#cb2543fd-d20b-44ff-af6b-9ceae86c61cd)

- **GB-->BE (Day ahead 16/08/2023)** · HTTP 200
  - [S23](corpus-xml.md#s23) · 1 `TimeSeries`, 1 `Period`, 24 `Point`, A01.

### R31

Transmission · [12.1.F Commercial Schedules](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#0efc945f-4602-4db5-9fc7-79aad6f681e2)

- **BE-->FR (01/11/2024)** · HTTP 200
  - [S23](corpus-xml.md#s23) · 2 `TimeSeries`, 2 `Period`, 47 `Point`, A03.
- **Legacy API: DE-LU-->FR (24/08/2023)** · HTTP 200
  - [S23](corpus-xml.md#s23) · 2 `TimeSeries`, 2 `Period`, 48 `Point`, A01.

### R32

Transmission · [12.1.F Commercial Schedules - Net Positions](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#5064248a-46be-4b4e-9db6-5e7dbb829f79)

- **AT (11/06/2025)** · HTTP 200
  - [S24](corpus-xml.md#s24) · 10 `TimeSeries`, 10 `Period`, 69 `Point`, A03.

### R33

Transmission · [11.3 Cross Border Capacity of DC Links - Intraday Transfer Limits](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#a95965c2-362f-4286-8d97-f84b3b36cabc)

- **GB(ElecLink)-->FR (16/08/2023)** · HTTP 200
  - [S23](corpus-xml.md#s23) · 1 `TimeSeries`, 1 `Period`, 24 `Point`, A01.

### R34

Transmission · [13.1.C Costs of Congestion Management](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#e5934604-3592-43f1-924c-3fb636e81026)

- **BE (2022)** · HTTP 200
  - [S29](corpus-xml.md#s29) · 36 `TimeSeries`, 36 `Period`, 36 `Point`, A01.

### R35

Transmission · [13.1.A Redispatching Internal](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#5bf1770e-06ab-4306-b2c9-cd3e40009e02)

- **NL (Nov 2023)** · HTTP 200
  - [S29](corpus-xml.md#s29) · 2 `TimeSeries`, 2 `Period`, 192 `Point`, 2 `Reason`, A01.

### R36

Transmission · [13.1.A Redispatching Cross Border](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#ef002d57-8582-4b86-a18c-fc2bd7072ae1)

- **AT-->FR (Nov 2023)** · HTTP 200
  - [S29](corpus-xml.md#s29) · 1 `TimeSeries`, 1 `Period`, 44 `Point`, 2 `Reason`, A01.

### R37

Transmission · [9.1 Expansion and Dismantling Project](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#b15b8389-d8f1-4a14-b29a-69b43d268b14)

- **HU-->SK (2023)** · HTTP 200
  - [S29](corpus-xml.md#s29) · 2 `TimeSeries`, 2 `Period`, 2 `Point`, A01.

### R38

Transmission · [13.1.B Countertrading](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#5726e914-6c4c-4d5c-8aba-c22b39eaf210)

- **ES-->FR (13/09/2023)** · HTTP 200
  - [S29](corpus-xml.md#s29) · 6 `TimeSeries`, 6 `Period`, 17 `Point`, 6 `Reason`, A01.

### R39

Outages · [15.1.C-D Unavailability of Production Units](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#79fa460a-8463-4247-8f54-d30062a4154b)

- **BE (01/2023)** · HTTP 200
  - 001-001-PLANNED_UNAVAIL_OF_PRODUCTION_UNITS_202301200900-202301201700.xml · [S31](corpus-xml.md#s31) · 1 `TimeSeries`, 1 `Available_Period`, 1 `Point`, 1 `Reason`, A03.
  - 002-002-PLANNED_UNAVAIL_OF_PRODUCTION_UNITS_202301230900-202301231700.xml · Nom seulement.
- **15.1.C-D Unavailability of Production Units** · HTTP 200
  - ZIP binaire altéré dans l’export.

### R40

Outages · [15.1.A&B Unavailability of Generation Units](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#1d54f9eb-820b-4c0b-b7ad-ba0dc1cc50ac)

- **BE (03/01/2023)** · HTTP 200
  - 001-PLANNED_UNAVAIL_OF_GENERATION_UNITS_201512312300-201601022300.xml · [S31](corpus-xml.md#s31) · 1 `TimeSeries`, 2 `Available_Period`, 2 `Point`, 1 `Reason`, A03.
  - 002-PLANNED_UNAVAIL_OF_GENERATION_UNITS_201512312300-201601022300.xml · Nom seulement.

### R41

Outages · [7.1.A-B Aggregated Unavailability of Consumption Units](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#50e47a07-24a9-4beb-a910-a54f78ff3502)

- **DE-LU (Nov 2023)** · HTTP 200
  - [S31](corpus-xml.md#s31) · 4 `TimeSeries`, 4 `Available_Period`, 4 `Point`, 1 `Reason`, A03.

### R42

Outages · [10.1.A&B Unavailability of Transmission Infrastructure](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#f60d9fb3-43a7-4532-859d-a5a5527b8d94)

- **FR-->BE (02/12/2023)** · HTTP 200
  - 001-001-PLANNED_UNAVAIL_IN_TRANSMISSION_GRID_202307011430-202312311700.xml · [S31](corpus-xml.md#s31) · 1 `TimeSeries`, 26 `Available_Period`, 39 `Point`, 1 `Reason`, A03.
  - 002-002-PLANNED_UNAVAIL_IN_TRANSMISSION_GRID_202310230730-202312221700.xml · Nom seulement.
  - 007-007-CHANGES_IN_ACTUAL_AVAIL_IN_TRANSMISSION_GRID_202311282000-202312121535.xml · Nom seulement.

### R43

Outages · [10.1.A&B Unavailability of Transmission Infrastructure - Available Capacity](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#362e6f9b-3025-4435-b63e-7857599296d0)

- Exemple officiel pour le test · [Unavailability in the Transmission Grid – Available Capacity \[10.1.A&B\] - XSD4:2.xml](https://gitlab.entsoe.eu/transparency/xml-examples/-/blob/b4a79f784db970feb863e5e7869aa98d036b2993/Outages/Unavailability%20in%20the%20Transmission%20Grid%20%E2%80%93%20Available%20Capacity%20%5B10.1.A%26B%5D%20-%20XSD4%3A2.xml) · [S33](corpus-xml.md#s33) · 1 `TimeSeries`, 1 `Available_Period`, 1 `Point`, 1 `Reason`, A03.

### R44

Outages · [10.1.A&B Unavailability of Transmission Infrastructure - Net Position Impact](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#9ef4ee96-23cc-4470-82b2-ed5fdbb03f0c)

- Exemple officiel pour le test · [Unavailability in the Transmission Grid – Net Position Impact \[10.1.A&B\] - XSD4:2.xml](https://gitlab.entsoe.eu/transparency/xml-examples/-/blob/b4a79f784db970feb863e5e7869aa98d036b2993/Outages/Unavailability%20in%20the%20Transmission%20Grid%20%E2%80%93%20Net%20Position%20Impact%20%5B10.1.A%26B%5D%20-%20XSD4%3A2.xml) · [S33](corpus-xml.md#s33) · 1 `TimeSeries`, 1 `Available_Period`, 2 `Point`, 1 `Reason`, A03.

### R45

Outages · [10.1.C Unavailability of Offshore Grid Infrastructure](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#529ca7e7-9e89-412a-93d8-fb225ede5df7)

- **DE-LU (15/01/2023)** · HTTP 200
  - 001-001-CHANGES_IN_ACTUAL_AVAIL_OF_OFFSHORE_GRID_202301151402-202301152024.xml · [S31](corpus-xml.md#s31) · 1 `TimeSeries`, 6 `Point`, 1 `Reason`, A03.
  - 002-002-CHANGES_IN_ACTUAL_AVAIL_OF_OFFSHORE_GRID_202301151402-202301152024.xml · Nom seulement.

### R46

Outages · [Fall-backs \[IFs IN 7.2, mFRR 3.11, aFRR 3.10\]](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#cb4ba59d-0dd5-4b43-b2d4-4b6d18c83c94)

- **BE (aFRR disconnection 2023)** · HTTP 200
  - 001-FALL_BACKS_202307210000-202307211445.xml · [S32](corpus-xml.md#s32) · 1 `TimeSeries`, 1 `Reason`, A03.
  - 002-FALL_BACKS_202308240000-202308240030.xml · Nom seulement.

### R47

Balancing · [17.1.F Prices of Activated Balancing Energy](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#c301d91e-53ac-4aca-8e18-f29e9146c4a6)

- **BE (aFRR 04/09/2023)** · HTTP 200
  - [S04](corpus-xml.md#s04) · 4 `TimeSeries`, 4 `Period`, 316 `Point`, A03.

### R48

Balancing · [IF aFRR 3.16 Cross Border Marginal Prices (CBMPs) for aFRR Central Selection (CS)](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#d8e9ac28-1687-4339-bc18-7afad2bf028c)

- **50Hertz (09/11/2023)** · HTTP 200
  - [S04](corpus-xml.md#s04) · 1 `TimeSeries`, 1 `Period`, 1014 `Point`, A03.

### R49

Balancing · [12.3.B&C Balancing energy bids](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#71d80400-ba94-4c37-9405-82b2d2bc0962)

- **CZ (mFRR 02/10/2024)** · HTTP 200
  - 001-001-PLANNED_UNAVAIL_OF_PRODUCTION_UNITS_202301200900-202301201700.xml · [S25](corpus-xml.md#s25) · 100 `Period`, 100 `Point`.
- Le nom du membre dans l’exemple évoque une indisponibilité. Sa racine identifie un document d’offres d’équilibrage.

### R50

Balancing · [12.3.B&C Balancing energy bids archives](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#f98476ea-43b8-4b25-9ac6-728f296a2bb2)

- **BE (mFRR 08/10/2023)** · HTTP 200
  - mFRR_BE_20231007T2200Z-20231008T0200Z.zip · ZIP membre mentionné.
  - mFRR_BE_20231007T2200Z-20231008T0200Z.zip / Up_Specific_None_NIL_001.xml · [S25](corpus-xml.md#s25) · 5 `Period`, 5 `Point`.
  - mFRR_BE_20231007T2200Z-20231008T0600Z.zip · ZIP membre mentionné.
  - mFRR_BE_20231008T1800Z-20231008T2200Z.zip · ZIP membre mentionné.

### R51

Balancing · [IFs mFRR 9.9, aFRR 9.6&9.8 Changes to Bid Availability](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#ebcb653a-f680-447f-8559-b60ca7f5264a)

- **DE(50Hertz) (24/09/2023)** · HTTP 200
  - [S07](corpus-xml.md#s07) · 1 `Reason`.

### R52

Balancing · [IFs mFRR 9.9, aFRR 9.6&9.8 Changes to Bid Availability Archives](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#acacb3ce-85e4-4eb9-8b47-03fc313507cb)

- **DE(50Hertz) (24/09/2023)** · HTTP 200
  - [S07](corpus-xml.md#s07) · 1 `Reason`.
- La fiche décrit un ZIP. La réponse enregistrée donne un XML directement exploitable pour construire le membre du ZIP de test.

### R53

Balancing · [12.3.E Aggregated Balancing Energy Bids (GL EB)](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#eb6e6efc-f941-4add-8dbd-0c60a95e3c02)

- **AT (aFRR 03/09/2023)** · HTTP 200
  - [S04](corpus-xml.md#s04) · 2 `TimeSeries`, 2 `Period`, 177 `Point`, A03.

### R54

Balancing · [IFs 3.10, 3.16 & 3.17 Netted and Exchanged Volumes](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#a6f759ce-f339-4d4a-8e75-125d2df38837)

- **50Hertz (IN 02/01/2023)** · HTTP 200
  - 001-NETTED_AND_EXCHANGED_VOLUMES_201512312300-201601022300.xml · [S04](corpus-xml.md#s04) · 2 `TimeSeries`, 13 `Period`, 96 `Point`, A01.

### R55

Balancing · [IFs 3.10, 3.16 & 3.17 Netted and Exchanged Volumes per Border](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#5a30a66e-be3e-4cef-9a38-6ca53ade6235)

- **BE-->FR (01/03/2025)** · HTTP 200
  - [S04](corpus-xml.md#s04) · 1 `TimeSeries`, 1 `Period`, 96 `Point`, A01.

### R56

Balancing · [IFs aFRR 3.4 & mFRR 3.4 Elastic Demands](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#7c20dd79-b4d3-405a-beee-04f4a6a8627b)

- **CZ (01/12/2023)** · HTTP 200
  - [S26](corpus-xml.md#s26) · 1 `Period`, 1 `Point`.

### R57

Balancing · [17.1.B&C Volumes and Prices of Contracted Reserves](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#3daf6d5b-1bd6-4c96-ae7d-56bc601e74bb)

- **CZ (Daily aFRR 25/09/2023)** · HTTP 200
  - 001-AMOUNT_AND_PRICES_PAID_OF_BALANCING_RESERVES_UNDER_CONTRACT_R3_202309242200-202309252200.xml · [S05](corpus-xml.md#s05) · 1 `TimeSeries`, 1 `Period`, 2 `Point`, A03.
- **R2: CZ (Daily aFRR 25/09/2023)** · HTTP 200
  - [S05](corpus-xml.md#s05) · 2 `TimeSeries`, 2 `Period`, 48 `Point`, A01.

### R58

Balancing · [12.3.F Procured balancing capacity (GL EB)](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#5acdefd7-94d6-4300-8077-ad1b4e0ce6d6)

- **50Hertz (aFRR 15/06/2023 00:00-01:00 UTC)** · HTTP 200
  - [S04](corpus-xml.md#s04) · 100 `TimeSeries`, 100 `Period`, 1600 `Point`, A01.
- La fiche décrit un ZIP. La réponse enregistrée donne un XML directement exploitable pour construire le membre du ZIP de test.

### R59

Balancing · [187.2 FCR Total capacity (SO GL)](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#c23b1aa3-fb8c-44d6-9fd0-354f0df9ce2c)

- **Continental Europe (2024)** · HTTP 200
  - [S06](corpus-xml.md#s06) · 1 `TimeSeries`, 1 `Period`, 1 `Point`, A03.

### R60

Balancing · [187.2 Shares of FCR capacity (SO GL)](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#df46d9b8-4f53-4ef0-81c4-78fe43c7579c)

- **FR (2024)** · HTTP 200
  - [S06](corpus-xml.md#s06) · 1 `TimeSeries`, 1 `Period`, 1 `Point`, A03.

### R61

Balancing · [190.2 Sharing of FCR between SAs (SO GL)](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#4a23c73c-3688-4358-bc73-78076e01feba)

- **IE-SEM (31/10/2015 - 16/12/2015)** · HTTP 200
  - [S06](corpus-xml.md#s06) · 1 `TimeSeries`, 1 `Period`, 1 `Point`, A03.

### R62

Balancing · [188.3 & 189.2 FRR & RR Capacity Outlook (SO GL)](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#7872c461-1220-42bd-9960-6a52e9472850)

- **AT (FRR - 2024)** · HTTP 200
  - [S06](corpus-xml.md#s06) · 2 `TimeSeries`, 2 `Period`, 8 `Point`, A01.

### R63

Balancing · [188.4 & 189.3 FRR and RR Actual Capacity (SO GL)](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#3fe7c6b7-bb42-4f8d-b73b-d7c072b3d8a2)

- **AT (FRR - Q1-2024)** · HTTP 200
  - [S06](corpus-xml.md#s06) · 6 `TimeSeries`, 6 `Period`, 6 `Point`, A01.

### R64

Balancing · [189.2 Outlook of Reserve Capacities on RR (SO GL)](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#0cfd09a2-bf95-41e3-aae5-c552bbd52c91)

- Exemple officiel pour le test · [Outlook of Reserve Capacities on RR \[SO GL 189.2\] - XSD4:5.xml](https://gitlab.entsoe.eu/transparency/xml-examples/-/blob/b4a79f784db970feb863e5e7869aa98d036b2993/Balancing/Outlook%20of%20Reserve%20Capacities%20on%20RR%20%5BSO%20GL%20189.2%5D%20-%20XSD4%3A5.xml) · [S06](corpus-xml.md#s06) · 2 `TimeSeries`, 2 `Period`, 8 `Point`, A01.

### R65

Balancing · [189.3 RR Actual Capacity(SO GL)](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#7ad50f5e-733a-49b9-bdbf-b3327ad69965)

- Exemple officiel pour le test · [RR Actual Capacity \[SO GL 189.3\] - XSD4:5.xml](https://gitlab.entsoe.eu/transparency/xml-examples/-/blob/b4a79f784db970feb863e5e7869aa98d036b2993/Balancing/RR%20Actual%20Capacity%20%5BSO%20GL%20189.3%5D%20-%20XSD4%3A5.xml) · [S06](corpus-xml.md#s06) · 6 `TimeSeries`, 6 `Period`, 6 `Point`, A01.

### R66

Balancing · [17.1.I Financial Expenses and Income for Balancing](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#411f8491-b9ed-4b87-a48a-f0e79f88c040)

- **HU (Feb 2023)** · HTTP 200
  - 001-FINANCIAL_EXPENSES_AND_INCOME_FOR_BALANCING_202302010000-202303010000.xml · [S03](corpus-xml.md#s03) · 1 `TimeSeries`, 1 `Period`, 1 `Point`, A01.
  - 002-PLANNED_UNAVAIL_OF_GENERATION_UNITS_201512312300-201601022300.xml · Nom seulement.

### R67

Balancing · [12.3.H&I Allocation and use of cross-zonal balancing capacity](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#a39ff082-5e7f-4789-9de4-f55e7cabbdb4)

- **SE4-->DK2 (aFRR 15/06/2023)** · HTTP 200
  - [S04](corpus-xml.md#s04) · 4 `TimeSeries`, 4 `Period`, 96 `Point`, A01.
- La fiche décrit un ZIP. La réponse enregistrée donne un XML directement exploitable pour construire le membre du ZIP de test.

### R68

Balancing · [IFs 4.5 Permanent Allocation Limitations to Cross-border Capacity on HVDC Lines](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#c82476b7-fcd7-4ccf-9d28-9b9864b1983f)

- **NL-->DK1 (2021)** · HTTP 200
  - [S19](corpus-xml.md#s19) · 1 `TimeSeries`, 1 `Reason`.

### R69

Balancing · [IFs 4.3 & 4.4 Balancing Border Capacity Limitations](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#e5d8685b-09c8-48b9-be9c-109fad6fb179)

- **AT-->CZ (mFRR 01/02/2024)** · HTTP 200
  - [S09](corpus-xml.md#s09) · 1 `TimeSeries`, 1 `Period`, 44 `Point`, A01.

### R70

Balancing · [190.1 Sharing of RR and FRR (SO GL)](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#237b7065-fa66-4622-9cd8-d36dbc396b64)

- Exemple officiel pour le test · [Sharing RR and FRR \[SO GL 190.1\] - XSD4:5.xml](https://gitlab.entsoe.eu/transparency/xml-examples/-/blob/b4a79f784db970feb863e5e7869aa98d036b2993/Balancing/Sharing%20RR%20and%20FRR%20%5BSO%20GL%20190.1%5D%20-%20XSD4%3A5.xml) · [S06](corpus-xml.md#s06) · 1 `TimeSeries`, 1 `Period`, 1 `Point`, A03.

### R71

Balancing · [190.3 Exchanged Reserve Capacity (SO GL)](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#b77eb3fc-d58e-4173-98ff-3ee259b5bafd)

- Exemple officiel pour le test · [Exchanged reserve Capacity \[SO GL 190.3\] - XSD4:5.xml](https://gitlab.entsoe.eu/transparency/xml-examples/-/blob/b4a79f784db970feb863e5e7869aa98d036b2993/Balancing/Exchanged%20reserve%20Capacity%20%5BSO%20GL%20190.3%5D%20-%20XSD4%3A5.xml) · [S06](corpus-xml.md#s06) · 1 `TimeSeries`, 1 `Period`, 1 `Point`, A03.

### R72

Balancing · [17.1.G Imbalance prices](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#f68ce7bd-c82d-4d79-9cb7-4808f84ed948)

- **NL (01/11/2023)** · HTTP 200
  - 001-IMBALANCE_PRICES_202310312300-202311112300.xml · [S05](corpus-xml.md#s05) · 22 `TimeSeries`, 22 `Period`, 2112 `Point`, A01.

### R73

Balancing · [17.1.H Total Imbalance Volumes](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#c87c5b5f-61ad-4e8f-a9fe-45d1c81e2f04)

- **AT (04/11/2023)** · HTTP 200
  - 001-TOTAL_IMBALANCE_VOLUMES_202311032300-202311042300.xml · [S03](corpus-xml.md#s03) · 2 `TimeSeries`, 24 `Period`, 96 `Point`, A01.

### R74

Balancing · [12.3.A Current balancing state \[GL EB\]](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#71bbf0bc-5149-48d0-b64f-86500dadfb0d)

- **HU (30/05/2024)** · HTTP 200
  - [S04](corpus-xml.md#s04) · 3 `TimeSeries`, 3 `Period`, 1440 `Point`, A01.

### R75

Balancing · [185.4 Results of the Criteria Application Process - Measurements (SO GL)](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#3affa3bb-b469-443c-941e-aa49dad90689)

- **LFB\|CZ (Q4-2022)** · HTTP 200
  - [S06](corpus-xml.md#s06) · 14 `TimeSeries`, 14 `Period`, 34 `Point`, 2 `Reason`, A01.
- **SNA\|Continental Europe (Q3-2022)** · HTTP 200
  - [S06](corpus-xml.md#s06) · 16 `TimeSeries`, 16 `Period`, 40 `Point`, 1 `Reason`, A01.

### R76

Master Data · [Production and Generation Units](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#dbff7cb1-04f1-42d3-b8ae-bfb105a58b14)

- **BE (01/01/2017)** · HTTP 200
  - [S12](corpus-xml.md#s12) · 30 `TimeSeries`.

### R77

OMI · [Other Market Information](https://documenter.getpostman.com/view/7009892/2s93JtP3F6#c0941c12-e1fd-4f45-9de6-eaa15851d8b8)

- **TenneT-DE (24/09/2024)** · HTTP 200
  - 001-OTHER_MARKET_INFORMATION202409182200-202512152259.xml · [S21](corpus-xml.md#s21) · `publication_DateAndOrTime.dateTime`, `start_DateAndOrTime.dateTime`, `end_DateAndOrTime.dateTime`, `reason.code`, `reason.text`.

## Empreinte de l’export analysé

`61ab0665d73387583bca0b49453b8f589b23b0bb3429bf19904680ebb086577b`
