// netlify/functions/actualizar-stock.js
exports.handler = async () => {
  try {
    const STORE   = process.env.SHOPIFY_STORE;        // https://tutienda.myshopify.com
    const TOKEN   = process.env.SHOPIFY_API_KEY;      // shpat_...
    const VERSION = process.env.SHOPIFY_API_VERSION || '2024-07';
    const FIXED_LOCATION_ID = process.env.LOCATION_ID; // opcional: si ya la conoces

    const missing = [];
    if (!STORE) missing.push('SHOPIFY_STORE');
    if (!TOKEN) missing.push('SHOPIFY_API_KEY');
    try { new URL(STORE); } catch { missing.push('SHOPIFY_STORE (URL inválida; incluye https://)'); }
    if (missing.length) {
      return { statusCode: 500, body: JSON.stringify({ error: 'Faltan variables', missing }) };
    }

    // === hoy en America/Lima como YYYY-MM-DD ===
    const todayYMD = new Intl.DateTimeFormat('en-CA', {
      timeZone: 'America/Lima', year: 'numeric', month: '2-digit', day: '2-digit'
    }).format(new Date()); // en-CA => YYYY-MM-DD

    const headers = {
      'X-Shopify-Access-Token': TOKEN,
      'Content-Type': 'application/json'
    };

    const updated = [];
    const skipped = [];

    // 1) Productos (solo 50; si necesitas más, implementa paginación con page_info)
    const pRes = await fetch(`${STORE}/admin/api/${VERSION}/products.json?limit=50`, { headers });
    if (!pRes.ok) {
      return { statusCode: pRes.status, body: JSON.stringify({ step: 'fetch_products', text: await pRes.text() }) };
    }
    const { products = [] } = await pRes.json();

    // Helper para obtener 1 location si no está fijada por env
    async function getAnyLocationId() {
      if (FIXED_LOCATION_ID) return Number(FIXED_LOCATION_ID);
      const lRes = await fetch(`${STORE}/admin/api/${VERSION}/locations.json`, { headers });
      if (!lRes.ok) throw new Error(`No pude leer locations: ${await lRes.text()}`);
      const { locations = [] } = await lRes.json();
      if (!locations.length) throw new Error('No hay locations en la tienda');
      return locations[0].id;
    }
    const locationId = await getAnyLocationId();

    for (const product of products) {
      const productId = product.id;

      // 2) Metafields del producto
      const mRes = await fetch(`${STORE}/admin/api/${VERSION}/products/${productId}/metafields.json`, { headers });
      if (!mRes.ok) {
        skipped.push({ productId, reason: 'metafields_fetch_failed', text: await mRes.text() });
        continue;
      }
      const { metafields = [] } = await mRes.json();
      const fechaMeta = metafields.find(m => m.namespace === 'custom' && m.key === 'fecha_disponibilidad');
      const stockMeta = metafields.find(m => m.namespace === 'custom' && m.key === 'stock_programado');

      if (!fechaMeta || !stockMeta) {
        skipped.push({ productId, reason: 'missing_metafields' });
        continue;
      }

      const fechaStr = String(fechaMeta.value).slice(0, 10);          // recorta a YYYY-MM-DD
      const cantidad = parseInt(stockMeta.value || '0', 10);

      if (fechaStr !== todayYMD) {
        skipped.push({ productId, reason: 'date_mismatch', fechaStr, todayYMD, cantidad });
        continue;
      }
      if (!cantidad || cantidad < 0) {
        skipped.push({ productId, reason: 'invalid_quantity', cantidad });
        continue;
      }

      const variant = product?.variants?.[0];
      if (!variant) {
        skipped.push({ productId, reason: 'no_variant' });
        continue;
      }

      const variantId = variant.id;
      const inventoryItemId = variant.inventory_item_id;

      // 3) Asegurar que la variante esté gestionada por Shopify
      if (variant.inventory_management !== 'shopify') {
        const vUpd = await fetch(`${STORE}/admin/api/${VERSION}/variants/${variantId}.json`, {
          method: 'PUT', headers,
          body: JSON.stringify({ variant: { id: variantId, inventory_management: 'shopify' } })
        });
        if (!vUpd.ok) {
          skipped.push({ productId, variantId, reason: 'enable_inventory_management_failed', text: await vUpd.text() });
          continue;
        }
      }

      // 4) Establecer stock en la location
      const setRes = await fetch(`${STORE}/admin/api/${VERSION}/inventory_levels/set.json`, {
        method: 'POST',
        headers,
        body: JSON.stringify({
          location_id: locationId,
          inventory_item_id: inventoryItemId,
          available: cantidad
        })
      });

      if (setRes.ok) {
        updated.push({ productId, variantId, inventoryItemId, locationId, setTo: cantidad, fecha: todayYMD });
      } else {
        skipped.push({ productId, variantId, reason: 'inventory_set_failed', text: await setRes.text() });
      }
    }

    return {
      statusCode: 200,
      body: JSON.stringify({ message: 'Run OK', todayYMD, updated, skipped }, null, 2)
    };

  } catch (err) {
    return { statusCode: 500, body: JSON.stringify({ error: err?.message || String(err) }) };
  }
};
