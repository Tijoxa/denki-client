# Cas de parsing ENTSO‑E

## Séries et intervalles

- Lire `TimeSeries/Period/timeInterval/start` dans la période courante. `period.timeInterval/start` au niveau document peut avoir une autre valeur. Respecter la casse de `Period` et `period` selon le schéma. Les métadonnées de série s’appliquent aux lignes de cette série. [Prix de l’énergie R13](postman-xml.md#r13) · [Production R24](postman-xml.md#r24)
- Dans chaque période, calculer la position à partir de 1 avec la résolution du document. Pour A01, un point représente un pas. Pour A03, un point marque un début de bloc. Traiter l’intervalle avant le premier point et les indications de valeur manquante selon le guide. Vérifier la fin du dernier bloc et la réinitialisation à chaque période. Tester aussi une fenêtre dont la durée locale varie à cause du changement d’heure. [Courbes A01 et A03](https://transparencyplatform.zendesk.com/hc/en-us/articles/30262342482961-CurveType-A01-vs-CurveType-A03)
- Dans `in_Domain.mRID`, le point fait partie du nom local et `codingScheme` est un attribut. Cet extrait d’une série doit donner la même projection si le namespace par défaut est remplacé par un préfixe. Tester aussi deux identifiants `mRID` à des niveaux différents. [Capacité nominée R02](postman-xml.md#r02)

  ```xml
  <TimeSeries xmlns="urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:0">
    <mRID>1</mRID>
    <in_Domain.mRID codingScheme="A01">10YBE----------2</in_Domain.mRID>
  </TimeSeries>
  ```

## Documents qui demandent une projection spécifique

- **Indisponibilités**. Préserver le `mRID`, la révision, les identifiants d’actif, le statut et les intervalles de disponibilité au niveau où ils sont déclarés. Tester plusieurs membres ZIP et plusieurs révisions pour le même identifiant. [Unités de production R39](postman-xml.md#r39) · [Capacité disponible R43](postman-xml.md#r43) · [Impact sur les positions nettes R44](postman-xml.md#r44)
- **Contraintes réseau**. Examiner les niveaux de contraintes et leurs répétitions avant de choisir le grain des lignes. Tester des enfants répétés avec des métadonnées différentes et conserver leurs identifiants de rattachement. [Allocations flow-based R10](postman-xml.md#r10) · [Archives flow-based R11](postman-xml.md#r11)
- **Référentiels et informations de marché**. Pour le référentiel de production, conserver les liens entre unités, les identifiants et la date d’effet. Pour OMI, relever les champs descriptifs et les liens de publication de l’exemple dans le ZIP. [Unités R76](postman-xml.md#r76) · [OMI R77](postman-xml.md#r77)
- **Requêtes et acquittements**. Le POST utilise des `AttributeInstanceComponent` répétés, chacun associant `attribute` et `attributeValue`. Un acquittement contient des `Reason` dont il faut conserver le code et le texte pour déterminer si la requête n’a aucune donnée ou a échoué. [POST R15](postman-xml.md#r15) · [Acquittements](corpus-xml.md#acknowledgements-4) · [Signalement](corpus-xml.md#problemstatementdocument-1)

## Conteneurs et erreurs

- Tester un vrai ZIP dont les membres XML portent des noms distincts. Pour un ZIP imbriqué, vérifier que la limite de pagination s’applique à l’unité indiquée dans la fiche. Les en-têtes des exemples peuvent annoncer un type générique. Utiliser la signature d’octets pour reconnaître le ZIP. [Offres d’équilibrage R49](postman-xml.md#r49) · [Archives R50](postman-xml.md#r50) · [Disponibilité R52](postman-xml.md#r52)
- Couvrir un acquittement sans données en HTTP 200 et en erreur HTTP, une erreur métier, un XML invalide et un ZIP invalide. Les diagnostics doivent conserver le statut HTTP, la cause et le chemin du membre concerné, avec un jeton masqué.
