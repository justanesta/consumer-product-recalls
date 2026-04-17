What APPRIL is: A REST endpoint over Oracle ORDS exposing the Office of Pesticide Programs' product registration database. 
  Single endpoint (https://ordspub.epa.gov/ords/pesticides/apprilapi/), JSON responses, SODA-style filter syntax, max 10K
  records per page. ~400K total records, ~50K active.                                                                        
                  
  What APPRIL has that's recall-adjacent:                                                                                    
  - STATUS field with values: Canceled, Conditionally Registered, Conditionally Reregistered, Registered, Reinstated,
  Reregistered                                                                                                               
  - STATUS_GROUP (Active / Inactive)
  - STATUS_DT — date of latest status action                                                                                 
  - RUP_FLAG / RUP_REASON — restricted-use designation (current hazard categorizations of registered products, not recalls)  
  
  What APPRIL is missing:                                                                                                    
  - Any "recall," "stop-sale," or "withdrawal" event field
  - Reason for cancellation — voluntary commercial decisions and safety-driven cancellations both show up as STATUS=Canceled 
  with no way to distinguish them                                                                                           
  - Pesticide enforcement actions (these live in SSURO — Stop Sale, Use, or Removal Orders — a separate EPA program not      
  exposed via APPRIL)                                                                                                  
                                                                                                                             
  Translation: A STATUS=Canceled filter on APPRIL would surface 10s of thousands of products, the vast majority of which were
   just discontinued by the manufacturer, not recalled for safety. Surfacing that to consumers as "recalls" would be wrong.  
                  
  Forward implication for our schema, even if EPA stays deferred: If you ever add EPA (or analogous regulatory-action feeds),
   they're not "recalls" in the same sense. The cleanest forward-compatible move is to anticipate an event_type discriminator
   on recall_event (e.g., 'RECALL' vs 'REGULATORY_ACTION' vs 'ENFORCEMENT_ORDER') — or to rename the table to safety_event   
  with event_type from the start. This costs us nothing now and saves a migration later. Worth deciding when we draft the
  actual silver DDL.

  What to ask in the email follow-up: does EPA OPP publish a separate feed for SSURO orders (Federal Register? Enforcement   
  bulletin scrape? OECA ECHO data?). That's the actual recall-equivalent for pesticides.