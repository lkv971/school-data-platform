select
    ServiceID,
    Service
from {{ source('lise', 'Services') }}