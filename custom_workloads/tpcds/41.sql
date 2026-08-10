
select  distinct(i_product_name)
 from item i1
 where i_manufact_id between 919 and 919+40 
   and (select count(*) as item_cnt
        from item
        where (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'azure' or i_color = 'cornsilk') and 
        (i_units = 'Case' or i_units = 'Lb') and
        (i_size = 'petite' or i_size = 'economy')
        ) or
        (i_category = 'Women' and
        (i_color = 'wheat' or i_color = 'lime') and
        (i_units = 'Dozen' or i_units = 'Each') and
        (i_size = 'extra large' or i_size = 'large')
        ) or
        (i_category = 'Men' and
        (i_color = 'medium' or i_color = 'magenta') and
        (i_units = 'Pallet' or i_units = 'N/A') and
        (i_size = 'N/A' or i_size = 'medium')
        ) or
        (i_category = 'Men' and
        (i_color = 'blush' or i_color = 'sienna') and
        (i_units = 'Box' or i_units = 'Oz') and
        (i_size = 'petite' or i_size = 'economy')
        ))) or
       (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'lavender' or i_color = 'chiffon') and 
        (i_units = 'Gross' or i_units = 'Cup') and
        (i_size = 'petite' or i_size = 'economy')
        ) or
        (i_category = 'Women' and
        (i_color = 'dodger' or i_color = 'yellow') and
        (i_units = 'Gram' or i_units = 'Tsp') and
        (i_size = 'extra large' or i_size = 'large')
        ) or
        (i_category = 'Men' and
        (i_color = 'peach' or i_color = 'brown') and
        (i_units = 'Ounce' or i_units = 'Ton') and
        (i_size = 'N/A' or i_size = 'medium')
        ) or
        (i_category = 'Men' and
        (i_color = 'firebrick' or i_color = 'olive') and
        (i_units = 'Dram' or i_units = 'Pound') and
        (i_size = 'petite' or i_size = 'economy')
        )))) > 0
 order by i_product_name
 limit 100;


