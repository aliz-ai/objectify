package com.googlecode.objectify.cache;

import lombok.Value;

import com.google.appengine.api.memcache.Expiration;
import com.google.appengine.api.memcache.MemcacheService.CasValues;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Value
public class GaeMemcacheServiceAdapter implements MemcacheService {

	com.google.appengine.api.memcache.MemcacheService delegate;

	@Value
	private static class IdentifiableValueAdapter implements IdentifiableValue {

		com.google.appengine.api.memcache.MemcacheService.IdentifiableValue wrapped;

		@Override
		public Object getValue() {
			return wrapped.getValue();
		}

		@Override
		public IdentifiableValue withValue(Object value) {
			throw new UnsupportedOperationException(
					"This method is actually never used, and it cannot even be implemented");
		}

		protected com.google.appengine.api.memcache.MemcacheService.IdentifiableValue unwrap() {
			return wrapped;
		}
	}

	@Override
	public Object get(String key) {
		return delegate.get(key);
	}

	@Override
	public Map<String, IdentifiableValue> getIdentifiables(Collection<String> keys) {
		Map<String, com.google.appengine.api.memcache.MemcacheService.IdentifiableValue> ivs = delegate.getIdentifiables(keys);
		
		// Figure out cold cache values
		Map<String, Object> cold = new HashMap<>();
		for (String key: keys)
			if (ivs.get(key) == null)
				cold.put(key, null);

		if (!cold.isEmpty())
		{
			// The cache is cold for those values, so start them out with nulls that we can make an IV for
			delegate.putAll(cold);

			try {
				Map<String, com.google.appengine.api.memcache.MemcacheService.IdentifiableValue> ivs2 = delegate.getIdentifiables(cold.keySet());
				ivs.putAll(ivs2);
			} catch (Exception ex) {
				// At this point we should just not worry about it, the ivs will be null and uncacheable
			}
		}
		
		return ivs.entrySet().stream()
				.collect(Collectors.toMap(e -> (String) e.getKey(), e -> new IdentifiableValueAdapter(e.getValue())));
	}

	@Override
	public Map<String, Object> getAll(Collection<String> keys) {
		return delegate.getAll(keys);
	}

	@Override
	public void put(String key, Object thing) {
		delegate.put(key, thing);
	}

	@Override
	public void putAll(Map<String, Object> values) {
		delegate.putAll(values);
	}

	@Override
	public Set<String> putIfUntouched(Map<String, CasPut> values) {
		return delegate
				.putIfUntouched(values.entrySet().stream().collect(Collectors.toMap(e -> (String) e.getKey(), e -> {
					CasPut value = e.getValue();
					return new CasValues(((IdentifiableValueAdapter) value.getIv()).unwrap(),
							value.getNextToStore(),
							value.getExpirationSeconds() == 0 ? null : Expiration.byDeltaSeconds(value.getExpirationSeconds()));
				})));
	}

	@Override
	public void deleteAll(Collection<String> keys) {
		delegate.deleteAll(keys);
	}

}
