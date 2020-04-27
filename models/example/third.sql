{{ config(materialized='view') }}
with source_data as (select *, 
	case 
		when attachment_type ilike '%link%' then true
		else false 
	end as brandblocks from social_integrations.facebook_page_posts limit 100)

select * from source_data
