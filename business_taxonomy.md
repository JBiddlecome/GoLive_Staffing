Business Type Taxonomy & Matching Strategy

Overview

This document defines a standardized taxonomy for categorizing client
businesses and outlines the recommended approach for matching similar
businesses within the application.

The system is built from two data sources: - Structured field
(industry) - Unstructured field (industry_other)

------------------------------------------------------------------------

1. Core Business Types (Structured Data)

-   Corporate Dining / Workplace Food Services
-   Schools / Education
-   Rehabilitation / Social Services
-   Private Clubs
-   Catering Companies
-   Healthcare Facilities
-   Senior Assisted Living
-   Entertainment Studios
-   Restaurants
-   Convention Centers
-   Stadiums / Arenas
-   Hotels
-   Private Events
-   Casinos
-   Production / Media
-   Staffing / Referral Partners

------------------------------------------------------------------------

2. Expanded Business Types (Normalized)

Event & Hospitality

-   Event Venues
-   Event Coordinators / Planners
-   Private Events
-   Nightclubs
-   Wineries
-   Catering Extensions (Food Trucks, Ghost Kitchens)

Food & Beverage

-   Cafes
-   Food Trucks
-   Meal Prep Services
-   Ghost Kitchens

Corporate / Office / Workspace

-   Offices
-   Coworking Spaces
-   Corporate Event Spaces
-   Management Companies

Nonprofit / Social Services

-   Nonprofits
-   Job Assistance / Career Centers
-   Apprenticeship Programs
-   Rehabilitation / Job Readiness

Residential / Living Spaces

-   Apartments
-   Private Residences
-   Senior Centers

Arts / Culture / Recreation

-   Museums
-   Art Galleries
-   Camps
-   Farms / Garden Venues

Retail / Commercial

-   Retail Stores
-   Manufacturing Companies

Media / Creative / Marketing

-   Production Companies
-   Podcast Companies
-   Marketing / PR Firms
-   Design Firms

Religious / Institutional

-   Religious Institutions / Temples

------------------------------------------------------------------------

3. Final Recommended Taxonomy

-   Corporate Dining / Workplace
-   Education
-   Healthcare / Senior Living
-   Hospitality
-   Events
-   Food & Beverage
-   Entertainment & Media
-   Nonprofit / Social Services
-   Residential / Facilities
-   Retail / Commercial
-   Arts / Culture / Recreation
-   Religious / Institutional
-   Staffing / Partners

------------------------------------------------------------------------

4. Data Normalization Requirements

-   Normalize casing
-   Correct spelling errors
-   Merge duplicates
-   Trim whitespace
-   Standardize synonyms

------------------------------------------------------------------------

5. Matching Algorithm Strategy

Use a composite similarity model:

-   Industry Match (Primary)
-   Business Type Similarity (Secondary)
-   Location Proximity
-   Optional Name Similarity

------------------------------------------------------------------------

6. Example Matching Logic

similarity_score = (industry_weight * industry_match) + (type_weight *
type_similarity) + (location_weight * proximity_score) + (name_weight *
semantic_similarity)

------------------------------------------------------------------------

7. Implementation Notes

-   Use enums for categories
-   Maintain mapping dictionary
-   Cache similarity results
-   Build UI around final taxonomy
