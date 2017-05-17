## https://bigquery.cloud.google.com/savedquery/592844875481:c76b64f7c951469f98c2ca24e2354a9d


// Title: DFP_ActivityGroup_LAEvents_v1
// Description: DFP_ActivityGroup_LAEvents_v1

// ==== BigQuery ====

  /////DFP ACTIVITY DATA 62 ROWS SINCE 2017-03-01
SELECT
  LastUpdated,
  Transaction_Date,
  '1139578605085513190' AS ParentSiteGroupID,
  //ActivityGroup,
  CASE
    WHEN LOWER(ActivityGroup) LIKE LOWER('%Communication Interne%') THEN 'Communication Interne - 195687'
    WHEN LOWER(ActivityGroup) LIKE LOWER('%TC-LA-Event-Conference Femmes Leaders-227809%') THEN 'Conference Femmes Leaders - 227809'
    WHEN LOWER(ActivityGroup) LIKE LOWER('%HOUSE - TC-LA - Event-Experience Client-230264 - Avril/Mai%') THEN 'Experience Client - 230264'
    WHEN LOWER(ActivityGroup) LIKE LOWER('%HOUSE%-%TC-LA%-%Event-FinTech%-%229396%-%Mars/Avril%') THEN 'FinTech - 229396'
    WHEN LOWER(ActivityGroup) LIKE LOWER('%HOUSE - TC-LA - Event-Commercialisation de Produits Innovants - 219608 - Mars/Avril%') THEN 'Commercialisation de Produits Innovants - 219608'
    WHEN LOWER(ActivityGroup) LIKE LOWER('%TC-LA-Event-CroissanceTransfert-214603%') THEN 'Croissance Transfert - 214603'
    WHEN LOWER(ActivityGroup) LIKE LOWER('%TC-LA-Event-Formations Femmes Leaders - 205873%') THEN 'Formations Femmes Leaders - 205873'
    WHEN LOWER(ActivityGroup) LIKE LOWER('%TC-LA-Event-Gestion des Espaces-224589%') THEN 'Gestion des Espaces - 224589'
    WHEN LOWER(ActivityGroup) LIKE LOWER('%TC-LA-Event-Marketing du Contenu-21958%') THEN 'Marketing du Contenu - 219581'
    WHEN LOWER(ActivityGroup) LIKE LOWER('%Les Affaires - House - TC-LA-Event-Programmation 2017%') THEN 'Programmation 2017'
    WHEN LOWER(ActivityGroup) LIKE LOWER('%TC-LA-Event-SantePsychologique-140513%') THEN 'Sante Psychologique - 140513'
    WHEN LOWER(ActivityGroup) LIKE LOWER('%TC-LA-Event-Sommet Marketing-210430%') THEN 'Sommet Marketing - 210430'
    WHEN LOWER(ActivityGroup) LIKE LOWER('%TC-LA-Event-Sommet Sur L’Energie-203640%') THEN 'Sommet sur L\'Energie - 203640'
    WHEN LOWER(ActivityGroup) LIKE LOWER('%TC-LA-Event-MarketingB2B-193016%') THEN 'Marketing B2B - 193016'
    WHEN LOWER(ActivityGroup) LIKE LOWER('%TC-LA-Event-StrategiesdeFormation-197928%') THEN 'Strategies de Formation - 197928'
    WHEN LOWER(ActivityGroup) LIKE LOWER('%Strategie de formation - 4e Edition - Campagne House%') THEN 'Strategies de Formation - 197928'
    WHEN LOWER(ActivityGroup) LIKE LOWER('%StrategiesdeFormation%') THEN 'Strategies de Formation - 197928'
    WHEN LOWER(ActivityGroup) LIKE LOWER('%TC-LA-Event-Securite Info-212309%') THEN 'Securite Info - 212309'
    WHEN LOWER(ActivityGroup) LIKE LOWER('%TC-LA-Event-Objectif Nord-222070%') THEN 'Objectif Nord - 222070'
    ELSE ActivityGroup
  END AS ActivityGroup,
  //ActivityGroupNameID,
  ActivityGroupID,
  Activity,
  SUM(THX_ClickConvs) AS THX_ClickConvs,
  SUM(THX_ViewConvs) AS THX_ViewConvs,
  SUM(RGT_ViewConvs) AS RGT_ViewConvs,
  SUM(RGT_ClickConvs) AS RGT_ClickConvs,
  SUM(INTEGER(ClickConversions)) AS Clicks,
  SUM(INTEGER(ViewConversions)) AS Views,
FROM (
  SELECT
    DATE(DATE_ADD(FORMAT_UTC_USEC(CURRENT_DATE()), 0, 'DAY')) AS LastUpdated,
    IF(RIGHT(Transaction_Date,
        3) CONTAINS '/',
      // IF DATE FORMAT IS AS 1/1/16 DATE(TIMESTAMP(LPAD(REGEXP_EXTRACT(Transaction_Date, '.*/([0-9]{1,2})$'),
            4,
            '20') + '-' + LPAD(REGEXP_EXTRACT(Transaction_Date, '^([0-9]{1,2}).*'),
            2,
            '0') + '-' + LPAD(REGEXP_EXTRACT(Transaction_Date, '.*/([0-9]{1,2})/.*'),
            2,
            '0'))),
      // IF DATE FORMAT IS AS 1/1/2016 IF(RIGHT(Transaction_Date,
          5) CONTAINS '/',
        DATE(TIMESTAMP(DATE(TIMESTAMP(REGEXP_EXTRACT(REPLACE(REPLACE(Transaction_Date,'-',''),'Unlimited',''), '.*/([0-9]{4})$') + '-' + LPAD(REGEXP_EXTRACT(REPLACE(REPLACE(Transaction_Date,'-',''),'Unlimited',''), '^([0-9]{1,2}).*'),
                  2,
                  '0') + '-' + LPAD(REGEXP_EXTRACT(REPLACE(REPLACE(Transaction_Date,'-',''),'Unlimited',''), '.*/([0-9]{1,2})/.*'),
                  2,
                  '0'))))),
        Transaction_Date)) AS Transaction_Date,
    ActivityGroup,
    RIGHT(ActivityGroup,
      6) AS ActivityGroupNameID,
    Activity,
    CASE
      WHEN ActivityGroup = 'Campaign Croissance PME - Campagne Analyse keywords' THEN '000001'
      WHEN ActivityGroup = 'Les Affaires - House - TC-LA-Event-Programmation 2017' THEN '000003'
      WHEN ActivityGroup = 'Les Affaires - House - Catfish - TC-LA-Event-Marketing B2B' THEN '193016'
      WHEN ActivityGroup = 'Strategie de formation - 4e Edition - Campagne House' THEN '197928'
      WHEN ActivityGroup = 'TC-LA-Event-Communication Interne' THEN '195687'
      WHEN ActivityGroup = 'TC-LA-Event-Strategies de Formation' THEN '197928'
      ELSE REGEXP_REPLACE(ActivityGroup, r'(\D)+', '')
    END AS ActivityGroupID,
    //ActivityGroupID,
    //ActivityID,
    SUM(INTEGER(ViewThroughConversions)) AS ViewConversions,
    IF(LOWER(Activity) LIKE LOWER('%thank%'),
      SUM(INTEGER(ViewThroughConversions)),
      0) AS THX_ViewConvs,
    IF(LOWER(Activity) NOT LIKE LOWER('%thank%'),
      SUM(INTEGER(ViewThroughConversions)),
      0) AS RGT_ViewConvs,
    SUM(INTEGER(ClickThroughConversions)) AS ClickConversions,
    IF(LOWER(Activity) LIKE LOWER('%thank%'),
      SUM(INTEGER(ClickThroughConversions)),
      0) AS THX_ClickConvs,
    IF(LOWER(Activity) NOT LIKE LOWER('%thank%'),
      SUM(INTEGER(ClickThroughConversions)),
      0) AS RGT_ClickConvs,
    SUM(FLOAT(ConversionsPerClick)) AS ConversionsPerClick,
    SUM(FLOAT(AdvertiserViewThroughSales)) AS AdvertiserViewThroughSales,
    SUM(FLOAT(AdvertiserClickThroughSales)) AS AdvertiserClickThroughSales,
    SUM(INTEGER(TotalConversions)) AS TotalConversions,
    SUM(FLOAT(TotalAdvertiserSales)) AS TotalAdvertiserSales,
    //SUM(FLOAT(CPARevenue)) AS CPARevenue //
  FROM
    [extreme-pixel-830:ManagedServicesProcessed.APNXDFP_20170301]
  FROM (TABLE_DATE_RANGE( ManagedServices_DFP_Activity.DFP_ActivityGroup_LAEvents_,
        TIMESTAMP('2016-03-01'),
        TIMESTAMP('2017-12-31')))
  WHERE
    LOWER(Transaction_Date) <> LOWER('Total') //AND ActivityGroup LIKE //'%TC-LA-Event-Marketing du Contenu%' //'%TC-LA-Event-Gestion des Espaces%',
    //'%TC-LA-Event-Securite Info%',
    //'%TC-LA-Event-Objectif Nord%',
    //'%TC-LA-Event-Sommet Marketing%',
    //'%TC-LA-Event-CroissanceTransfert%',
    //'%TC-LA-Event-Formations Femmes Leaders%',
    //'%TC-LA-Event-SantePsychologique-140513%',
    //'%TC-LA-Event-Programmation 2017%',
    //'%TC-LA-Event-Sommet Sur Lâ€™Energie%',
    //'%Campagne - Sommet marketing B2B%',
  GROUP BY
    Transaction_Date,
    LastUpdated,
    ActivityGroup,
    ActivityGroupNameID,
    Activity,
    ActivityGroupID,
    //ActivityID,
  ORDER BY
    Transaction_Date DESC )
GROUP BY
  Transaction_Date,
  LastUpdated,
  ParentSiteGroupID,
  ActivityGroup,
  //ActivityGroupNameID,
  ActivityGroupID,
  Activity
ORDER BY
  Transaction_Date DESC ////SAVES AS ManagedServices_DFP_Activity.DFP_ActivityGroup_LAEvents_v1

// ==== JavaScript UDF ====

// Example user-defined function, documentation: https://goo.gl/6KR8O0
// Sample SQL: SELECT outputA, outputB FROM (passthrough(SELECT "abc" AS inputA, "def" AS inputB))

/*
function passthroughExample(row, emit) {
  emit({outputA: row.inputA, outputB: row.inputB});
}

bigquery.defineFunction(
  'passthrough',                           // Name of the function exported to SQL
  ['inputA', 'inputB'],                    // Names of input columns
  [{'name': 'outputA', 'type': 'string'},  // Output schema
   {'name': 'outputB', 'type': 'string'}],
  passthroughExample                       // Reference to JavaScript UDF
);
*/
