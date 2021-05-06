SELECT DISTINCT 
        bo.BgOrderID [Screening Direct Order #]
    ,   bo.Created  [Screening Direct Order Date]
    ,   CASE WHEN ISNULL(bo.BgOrderID , '') <> ''
                THEN sl.LookupText
             WHEN ISNULL(bo.BgOrderID , '') = '' AND ISNULL(boi.BgOrderID , '') <> ''
                THEN 'Missing/Deleted'
        END AS [Screening Direct Order Status]
    ,   ISNULL(c.Company, cc.company) [Screening Direct Customer], 
    ,   t.stripe_date [Stripe Payment Date], 
    ,   t.stripe_price [Stripe Price]
    --bei.InviteID 
    ,   t.stripe_payment_id [Screening Direct Payment Unique identifier]
    ,   CASE WHEN a.Currency = 0 
                THEN 'US' 
            WHEN a.Currency = 1
                THEN 'UK' 
        END AS Currency
    ,   CASE WHEN bei.[Status] = 0
                THEN 'Client created but never sent the invite' 
            WHEN bei.[Status] = 4
                THEN 'Undeliverable' 
            WHEN bei.[Status] = 5
                THEN 'Delivered' 
            WHEN bei.[Status] = 10
                THEN 'Ready'                   
            WHEN bei.[Status] = 200
                THEN 'Archive'                     
            ELSE 'Expired'
        END as [Invite Status]
    ,   t.reference_id
    ,   t.current_status
    ,   CASE WHEN boi.BgOrderID = bo.BgOrderID
                THEN ''
            ELSE
                boi.BgOrderID
        END AS MissingOrderID
    ,   CASE WHEN wfd.AccessCodeType = 0
                THEN 'One-Time Use'
            WHEN wfd.AccessCodeType = 1
                THEN ' Multi-Use'
        END AS AccessCodeType 
 FROM   
    [dbo].[BgEInvite] bei WITH (NOLOCK)
        JOIN #TMP t on bei.InviteID = t.inviteid
        LEFT JOIN [dbo].[BgOrderInvite] boi WITH (NOLOCK)
            ON t.[InviteID] = boi.[InviteID]
        LEFT JOIN [dbo].[vw_BgOrders] bo WITH (NOLOCK)
            ON boi.[BgOrderID] = bo.[BgOrderID]
        LEFT JOIN vw_SysLookup sl (nolock)
		    ON sl.LookupValue = bo.Status AND sl.Type = 53 -- BG Order Status 
		LEFT JOIN customers c (NOLOCK)
            ON bo.CustID = c.CustID
		LEFT JOIN customers cc (NOLOCK)
            ON bei.CustID = cc.CustID
        LEFT JOIN accounts a (nolock)
            ON bei.ActID = a.ActID
		LEFT JOIN WFDPortalAccessCodes wfd (nolock)
            ON wfd.AccessCodeID = bei.WFDPortalAccessCodeID
 ORDER BY t.stripe_payment_id

 DROP TABLE #tmp