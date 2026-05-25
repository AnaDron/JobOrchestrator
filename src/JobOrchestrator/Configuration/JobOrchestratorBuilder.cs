using JobOrchestrator.Configuration.Internal;
using Microsoft.Extensions.DependencyInjection;

namespace JobOrchestrator.Configuration;

/// <summary>Точка входа Fluent API для конфигурации SDK через <c>services.AddJobOrchestrator(jobs => {...})</c>.</summary>
public sealed class JobOrchestratorBuilder {
	/// <summary>Разделитель domain-префикса и имени стадии в полном FQN-имени (<c>"catalog:shops"</c>).</summary>
	internal const char DomainSeparator = ':';

	private readonly Dictionary<string, StageBuilder> _stages = new(StringComparer.Ordinal);
	private JobDefaults _defaults = new();
	private string? _currentDomain;

	/// <summary>
	/// <see cref="IServiceCollection"/>, переданный в <c>AddJobOrchestrator</c>. Доступен <b>только
	/// на probe-pass</b> (когда builder создаётся внутри extension'а с прямой ссылкой на services).
	/// На lazy-pass (DI-singleton builder, переиспользуемый <see cref="Hosting.IJobsConfigure.Apply"/>)
	/// — <c>null</c>: ServiceCollection уже заморожен, регистрации невозможны.
	/// <para>
	/// <b>Контракт backend-extension'ов</b> (<c>jobs.UseInMemoryStateStore()</c>,
	/// <c>jobs.UseRedisStateStore(...)</c> и т.п.):
	/// </para>
	/// <list type="number">
	/// <item><b>Обязательный no-op guard.</b> Первым делом extension <b>обязан</b> проверить
	/// <c>if (builder.Services is null) return builder;</c>. На lazy-pass регистрации уже выполнены
	/// на probe-pass — повторно их применять не нужно (и невозможно).</item>
	/// <item><b>Чтение TenantKey</b> для выбора между non-keyed и keyed-singleton-регистрацией:
	/// <c>TenantKey is null</c> → <c>Services.TryAddSingleton</c>, иначе <c>Services.TryAddKeyedSingleton(TenantKey)</c>.</item>
	/// <item><b>Идемпотентность через TryAdd*.</b> Повторный вызов того же extension'а под одним и
	/// тем же tenant'ом — no-op (first-wins). См. документацию SDK про backend-resolve-семантику.</item>
	/// </list>
	/// <para>
	/// Прямые мутации <see cref="Services"/> вне backend-extension-pattern'а (например,
	/// <c>builder.Services.Clear()</c> или подмена SDK-инфраструктуры) — undefined behavior. SDK не
	/// валидирует целостность ServiceCollection после возврата из configure-action.
	/// </para>
	/// </summary>
	public IServiceCollection? Services { get; }

	/// <summary>
	/// Tenant-ключ текущего orchestrator-а: непустой при keyed-регистрации
	/// (<c>services.AddJobOrchestrator(tenantKey, ...)</c>); <c>null</c> для одиночного
	/// <c>services.AddJobOrchestrator(...)</c>. Используется backend-extension'ами для выбора между
	/// non-keyed и keyed DI-регистрацией backend'а.
	/// </summary>
	public string? TenantKey { get; }

	/// <summary>
	/// Probe-pass конструктор: builder получает прямую ссылку на <see cref="IServiceCollection"/>
	/// и текущий <paramref name="tenantKey"/>. Вызывается из <c>AddJobOrchestrator</c>.
	/// </summary>
	internal JobOrchestratorBuilder(IServiceCollection services, string? tenantKey) {
		Services = services;
		TenantKey = tenantKey;
	}

	/// <summary>
	/// Default-конструктор для lazy-pass: builder создаётся DI как singleton (<c>AddSingleton&lt;JobOrchestratorBuilder&gt;()</c>)
	/// и переиспользуется аккумулятором <see cref="Hosting.IJobsConfigure"/>. На этом pass'е
	/// <see cref="Services"/> и <see cref="TenantKey"/> равны <c>null</c>; backend-регистрации уже выполнены
	/// на probe-pass, повторно их применять не нужно.
	/// </summary>
	public JobOrchestratorBuilder() {
		Services = null;
		TenantKey = null;
	}

	/// <summary>
	/// Дефолтные настройки, применяемые к стадиям в момент их объявления.
	/// Присваивание <c>null</c> пересоздаёт пустой <see cref="JobDefaults"/> — guard от случайного NRE в <c>StageBuilder</c>.
	/// </summary>
	public JobDefaults Defaults {
		get => _defaults;
		set => _defaults = value ?? new JobDefaults();
	}

	/// <summary>
	/// Вход в domain-блок (state-based). Все последующие <see cref="Stage"/>-вызовы до
	/// <see cref="WithoutDomain"/> получают префикс <c>"{domain}:"</c> в имени.
	/// <para>
	/// Вложенные domain'ы запрещены: повторный вызов без сброса бросает <see cref="JobConfigurationException"/>.
	/// Action-scoped вариант (<see cref="WithDomain(string, Action{JobOrchestratorBuilder})"/>) предпочтительнее —
	/// он гарантирует автоматический сброс state-а через <c>try/finally</c>.
	/// </para>
	/// </summary>
	public JobOrchestratorBuilder WithDomain(string domain) {
		EnterDomain(domain);
		return this;
	}

	/// <summary>
	/// Запускает <paramref name="configure"/> внутри domain-блока. Domain активен ТОЛЬКО на
	/// время выполнения action; восстанавливается через <c>try/finally</c> после выхода, даже при
	/// исключении внутри. Защищает от случая «забыли <see cref="WithoutDomain"/>».
	/// <para>
	/// Вложенные <c>WithDomain</c> любого стиля (state-based или action-scoped) — бросают
	/// <see cref="JobConfigurationException"/>.
	/// </para>
	/// </summary>
	public JobOrchestratorBuilder WithDomain(string domain, Action<JobOrchestratorBuilder> configure) {
		ArgumentNullException.ThrowIfNull(configure);
		// EnterDomain отвергает вложенные WithDomain (бросает при _currentDomain != null), поэтому
		// к моменту finally сохранять нечего — восстанавливаем безусловно в null.
		EnterDomain(domain);
		try {
			configure(this);
		} finally {
			_currentDomain = null;
		}
		return this;
	}

	/// <summary>
	/// Сбрасывает domain-state, установленный через <see cref="WithDomain(string)"/>.
	/// После — <see cref="Stage"/> возвращает имя без префикса. Идемпотентно: повторный вызов на
	/// уже-сброшенном state — no-op без exception.
	/// </summary>
	public JobOrchestratorBuilder WithoutDomain() {
		_currentDomain = null;
		return this;
	}

	/// <summary>Объявить новую стадию. Снимок текущих <see cref="Defaults"/> захватывается в этот момент.
	/// <para>
	/// Если активен domain-блок (<see cref="WithDomain(string)"/> или <see cref="WithDomain(string, Action{JobOrchestratorBuilder})"/>),
	/// реальное имя стадии — <c>"{domain}:{name}"</c>. Имя <paramref name="name"/> не должно содержать <c>':'</c>
	/// (разделитель зарезервирован для domain-префикса).
	/// </para>
	/// </summary>
	public IStageBuilder Stage(string name) {
		ArgumentException.ThrowIfNullOrEmpty(name);
		if (name.Contains(DomainSeparator)) {
			throw new JobConfigurationException(
				$"Имя стадии '{name}' содержит зарезервированный символ '{DomainSeparator}' — используйте WithDomain для префикса.");
		}
		var fullName = _currentDomain is null ? name : $"{_currentDomain}{DomainSeparator}{name}";
		if (_stages.ContainsKey(fullName)) {
			throw new JobConfigurationException($"Стадия с именем '{fullName}' уже объявлена.");
		}
		var sb = new StageBuilder(fullName, Defaults);
		_stages.Add(fullName, sb);
		return sb;
	}

	/// <summary>
	/// Валидация и установка <c>_currentDomain</c>. Общая для обоих стилей <c>WithDomain</c>.
	/// </summary>
	private void EnterDomain(string domain) {
		ArgumentException.ThrowIfNullOrEmpty(domain);
		if (domain.Contains(DomainSeparator)) {
			throw new JobConfigurationException(
				$"Domain '{domain}' содержит зарезервированный символ '{DomainSeparator}' — вложенные domain'ы не поддерживаются.");
		}
		if (_currentDomain is not null) {
			throw new JobConfigurationException(
				$"Domain уже установлен в '{_currentDomain}'. Вложенные WithDomain не поддерживаются — используйте WithoutDomain() перед сменой.");
		}
		_currentDomain = domain;
	}

	/// <summary>
	/// Собирает финальный <see cref="StageRegistry"/> с fully-initialized дескрипторами:
	/// валидация (<see cref="ConfigurationValidator"/>), bare-дескрипторы из builder-ов,
	/// затем <see cref="StageRegistry"/>-ctor досчитывает computed-поля над rawDeps.
	/// </summary>
	internal StageRegistry BuildRegistry() {
		var rawDeps = _stages.Values.ToDictionary(x => x.Name, x => x.Dependencies, StringComparer.Ordinal);
		ConfigurationValidator.Validate(_stages.Values, rawDeps);

		var descriptors = _stages.Values.Select(sb => new StageDescriptor {
			Name = sb.Name,
			ServiceType = sb.ServiceType!,
			Interval = sb.Interval,
			RetryPolicy = sb.RetryPolicy,
			Debounce = sb.Debounce,
			ExecutionTimeout = sb.ExecutionTimeout,
			ConcurrencyLimit = sb.ConcurrencyLimit,
		}).ToList();

		return new StageRegistry(descriptors, rawDeps, _defaults.GlobalConcurrencyLimit);
	}
}
