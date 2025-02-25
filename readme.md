# Database Documentation





## Tables and Foreign Keys



### Individuals and Roles

#### prosopography
**Foreign Keys**: None

#### individual_social_roles
**Foreign Keys**: 
- Source_ID (references `sources.UID`)
- Social_Role_ID (references `social_roles.UID`)
- Individual_ID (references `individuals.UID`)

#### social_roles
**Foreign Keys**: None

#### role_honorific
**Foreign Keys**: 
- Source_ID (references `sources.UID`)
- Role_ID (references `roles.UID`)
- Honorific_ID (references `honorifics.UID`)

#### honorifics
**Foreign Keys**: None

#### seals
**Foreign Keys**: 
- Source_ID (references `sources.UID`)
- Individual_ID (references `individuals.UID`)

### Sources and References
#### bibliography
**Foreign Keys**: 
- Repository_ID (references `repositories.UID`)

#### classical_sources
**Foreign Keys**: 
- Location_ID (references `locations.UID`)

#### references_to_classical_sources
**Foreign Keys**: 
- Source_ID (references `sources.UID`)
- Classical_ID (references `classical_sources.UID`)

#### references_to_individuals
**Foreign Keys**: 
- Source_ID (references `sources.UID`)
- Individual_ID (references `individuals.UID`)

#### references_to_locations
**Foreign Keys**: 
- Source_ID (references `sources.UID`)
- Location_ID (references `locations.UID`)

#### related_sources
**Foreign Keys**: 
- Referencing_Source_ID (references `sources.UID`)
- Referenced_Source_ID (references `sources.UID`)

#### repositories
**Foreign Keys**: 
- Location_ID (references `locations.UID`)

#### copies_holdings
**Foreign Keys**: 
- Scribe_Individual_ID (references `individuals.UID`)
- Repository_ID (references `repositories.UID`)
- Reference_Source_ID (references `sources.UID`)
- Location_ID (references `locations.UID`)
- Copied_Source_ID (references `sources.UID`)
- Copied_Classical_ID (references `classical_sources.UID`)

### Locations
#### gazetteer
**Foreign Keys**: None

#### location_attributes
**Foreign Keys**: 
- Location_ID (references `locations.UID`)

#### location_hierarchies
**Foreign Keys**: 
- Tertiary_ID (references `locations.UID`)
- Source_ID (references `sources.UID`)
- Parent_ID (references `location_hierarchies.UID`)
- Child_ID (references `location_hierarchies.UID`)

#### conquests
**Foreign Keys**: 
- Defending_Power_ID (references `powers.UID`)
- Conquering_Power_ID (references `powers.UID`)
- Conquered_Territory_ID (references `territories.UID`)

### Knowledge and Genres
#### knowledge_branch
**Foreign Keys**: 
- Source_ID (references `sources.UID`)
- Parent_ID (references `knowledge_branch.UID`)
- Classical_ID (references `classical_sources.UID`)
- Child_ID (references `knowledge_branch.UID`)

#### knowledge_forms
**Foreign Keys**: None

#### knowledge_mastery
**Foreign Keys**: 
- Source_ID (references `sources.UID`)
- Knowledge_Form_ID (references `knowledge_forms.UID`)
- Individual_ID (references `individuals.UID`)

#### classical_genre
**Foreign Keys**: 
- Source_ID (references `sources.UID`)
- Knowledge_Form_ID (references `knowledge_forms.UID`)
- Classical_ID (references `classical_sources.UID`)

### Definitions and Terms
#### definitions
**Foreign Keys**: 
- Source_ID (references `sources.UID`)
- Social_Role_ID (references `social_roles.UID`)
- Lexicon_ID (references `lexicon.UID`)

#### related_terms
**Foreign Keys**: 
- Source_ID (references `sources.UID`)
- Parent_ID (references `related_terms.UID`)
- Child_ID (references `related_terms.UID`)

#### epochs
**Foreign Keys**: None

