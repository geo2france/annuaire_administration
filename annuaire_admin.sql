/* création de l'extension PGSQL */
CREATE EXTENSION file_fdw;

/* création du serveur distant */
CREATE SERVER annuaire_administration FOREIGN DATA WRAPPER file_fdw;

/* création de la table distante */
CREATE FOREIGN TABLE annuaire_admin_table (
	plage_ouverture jsonb,
	site_internet jsonb,
	copyright character varying,
	siren character varying,
	ancien_code_pivot character varying,
	reseau_social jsonb,
	texte_reference jsonb,
	partenaire character varying,
	telecopie character varying,
	nom character varying,
	siret character varying,
	itm_identifiant character varying,
	sigle character varying,
	affectation_personne jsonb,
	date_modification date,
	date_modification_datetime character varying,
	adresse_courriel character varying,
	service_disponible character varying,
	organigramme jsonb,
	pivot character varying,
	partenaire_identifiant character varying,
	ancien_identifiant character varying,
	id character varying,
	ancien_nom character varying,
	commentaire_plage_ouverture character varying,
	annuaire jsonb,
	tchat character varying,
	hierarchie jsonb,
	categorie character varying,
	sve character varying,
	telephone_accessible jsonb,
	application_mobile character varying,
	version_type character varying,
	type_repertoire character varying,
	telephone character varying,
	version_etat_modification character varying,
	date_creation date,
	date_creation_datetime character varying,
	partenaire_date_modification character varying,
	mission character varying,
	formulaire_contact character varying,
	version_source character varying,
	type_organisme character varying,
	code_insee_commune character varying,
	statut_de_diffusion character varying,
	adresse jsonb,
	url_service_public character varying,
	information_complementaire character varying,
	date_diffusion date
) 
SERVER annuaire_administration
OPTIONS ( program 'wget -q -O - "https://api-lannuaire.service-public.fr/api/explore/v2.1/catalog/datasets/api-lannuaire-administration/exports/csv"', format 'csv', delimiter ';', header 'true' );

/* création de la vue matérialisée */
create materialized view annuaire_admin_view as
SELECT
	row_number() OVER ()::integer AS gid,
    o.nom,
    o.plage_ouverture,
    o.code_insee_commune,
    o.adresse_courriel,
    o.date_modification,
    elem.value ->> 'longitude'::text AS longitude,
    elem.value ->> 'latitude'::text AS latitude,
    elem.value ->> 'numero_voie'::text AS adresse_postale,
    elem.value ->> 'code_postal'::text AS code_postale,
    elem.value ->> 'nom_commune'::text AS commune_postale,
    elem2.value ->> 'valeur'::text AS telephone,
    elem3.value ->> 'valeur'::text AS site_web,
    st_point(((elem.value ->> 'longitude'::text)::real)::double precision, ((elem.value ->> 'latitude'::text)::real)::double precision)::geometry(Point,4326) AS geom
   FROM donnees_externes.annuaire_admin o,
    LATERAL jsonb_array_elements(o.adresse) elem(value),
    LATERAL jsonb_array_elements(o.telephone) elem2(value),
    LATERAL jsonb_array_elements(o.site_internet) elem3(value)
  WHERE (o.code_insee_commune::text ~~ '80%'::text OR o.code_insee_commune::text ~~ '02%'::text OR o.code_insee_commune::text ~~ '60%'::text OR o.code_insee_commune::text ~~ '62%'::text OR o.code_insee_commune::text ~~ '59%'::text) AND (elem.value ->> 'type_adresse'::text) = 'Adresse'::text AND (elem.value ->> 'longitude'::text) <> ''::text;
