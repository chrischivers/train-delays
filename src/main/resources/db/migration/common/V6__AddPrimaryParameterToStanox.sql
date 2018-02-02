ALTER TABLE stanox
ADD COLUMN primary_entry BOOLEAN NULL;

UPDATE stanox
SET primary_entry = TRUE
WHERE (stanox_code = '01100' AND tiploc_code = 'IVRNESS')
OR (stanox_code = '30120' AND tiploc_code = 'PRST')
OR (stanox_code = '31510' AND tiploc_code = 'MNCRVIC')
OR (stanox_code = '40320' AND tiploc_code = 'CHST')
OR (stanox_code = '42159' AND tiploc_code = 'XXC')
OR (stanox_code = '52215' AND tiploc_code = 'STFORDI')
OR (stanox_code = '72277' AND tiploc_code = 'WLSDNJL')
OR (stanox_code = '82341' AND tiploc_code = 'YOVILJN')
OR (stanox_code = '86037' AND tiploc_code = 'WOKICHS')
OR (stanox_code = '86441' AND tiploc_code = 'BOGNORR')
OR (stanox_code = '86935' AND tiploc_code = 'POOLE')
OR (stanox_code = '86981' AND tiploc_code = 'WEYMTH')
OR (stanox_code = '87071' AND tiploc_code = 'EFNGHMJ')
OR (stanox_code = '87201' AND tiploc_code = 'VICTRIA')
OR (stanox_code = '87219' AND tiploc_code = 'CLPHMJN')
OR (stanox_code = '87261' AND tiploc_code = 'WIMBLDN')
OR (stanox_code = '87981' AND tiploc_code = 'BRGHMHS')
OR (stanox_code = '88486' AND tiploc_code = 'SWLY')
OR (stanox_code = '89428' AND tiploc_code = 'ASHFKY')
OR (stanox_code = '89530' AND tiploc_code = 'EBSFDOM')
;
