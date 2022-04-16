defmodule WadmTest do
  use ExUnit.Case
  import ExUnit.CaptureLog
  require Logger

  setup context do
    prefix = "default"
    host_id = "ND4MFQ4RUAV7S5IHBK5DJXS2HNTCWSA6NGVTSSQTESDI5VLJ4WBA5MLY"
    connection = %{host: '172.18.0.1', port: 4222, tls: false}
    {:ok, pid} = Gnat.start_link(connection)
    [prefix: prefix, host_id: host_id, control_nats: pid]
    # on_exit(fn ->
    #   :ok = Gnat.stop(context[:control_nats])
    # end)
  end

  test "test ping", context do
    assert is_pid(context[:control_nats])

    topic = "wasmbus.ctl.#{context[:prefix]}.ping.hosts"
    result = Gnat.request(context[:control_nats], topic, [], receive_timeout: 2_000)
    Logger.debug("#{__ENV__.file}:#{__ENV__.line} #{inspect(result)}")
    {:ok, %{body: body}} = result
    b = body |> Jason.decode!()
    Logger.debug("#{__ENV__.file}:#{__ENV__.line} #{inspect(Map.has_key?(b, "id"))}")

    assert true = Map.has_key?(b, "id")
  end

  test "test get link definitions", context do
    assert is_pid(context[:control_nats])

    topic = "wasmbus.ctl.#{context[:prefix]}.get.links"
    result = Gnat.request(context[:control_nats], topic, [], receive_timeout: 2_000)
    Logger.debug("#{__ENV__.file}:#{__ENV__.line} #{inspect(result)}")
    {:ok, %{body: body}} = result
    b = body |> Jason.decode!()
    Logger.debug("#{__ENV__.file}:#{__ENV__.line} #{inspect(Map.has_key?(b, "id"))}")

    assert true = Map.has_key?(b, "links")
    # assert :true = Map.has_key?(b,"links.provider_id")
  end

  test "actor auction", context do
    # finds a host, what's a valid ociref?
    assert is_pid(context[:control_nats])
    topic = "wasmbus.ctl.#{context[:prefix]}.auction.actor"

    payload =
      %{
        "constraints" => %{},
        "actor_ref" => "actor_ociref"
      }
      |> Jason.encode!()

    result = Gnat.request(context[:control_nats], topic, payload, receive_timeout: 2_000)
    Logger.debug("#{__ENV__.file}:#{__ENV__.line} #{inspect(result)}")
    {:ok, %{body: body}} = result
    b = body |> Jason.decode!()
    Logger.debug("#{__ENV__.file}:#{__ENV__.line} #{inspect(Map.has_key?(b, "id"))}")

    assert true = Map.has_key?(b, "host_id")
  end

  test "provider auction", context do
    # finds a host, what's a valid ociref?
    assert is_pid(context[:control_nats])
    topic = "wasmbus.ctl.#{context[:prefix]}.auction.provider"

    payload =
      %{
        "constraints" => %{},
        "provider_ref" => "provider_ociref",
        "link_name" => "default"
      }
      |> Jason.encode!()

    result = Gnat.request(context[:control_nats], topic, payload, receive_timeout: 2_000)
    Logger.debug("#{__ENV__.file}:#{__ENV__.line} #{inspect(result)}")
    {:ok, %{body: body}} = result
    b = body |> Jason.decode!()
    Logger.debug("#{__ENV__.file}:#{__ENV__.line} #{inspect(Map.has_key?(b, "id"))}")

    assert true = Map.has_key?(b, "host_id")
  end

  test "host invetory", context do
    assert is_pid(context[:control_nats])

    host_id = context[:host_id]
    # topic = "wasmbus.#{context[:prefix]}.get.#{host_id}.inv"
    # I am not sure how to handle the case of a topic not being present
    topic = "wasmbus.ctl.#{context[:prefix]}.get.#{host_id}.inv"

    result = Gnat.request(context[:control_nats], topic, [], receive_timeout: 2_000)
    Logger.debug("#{__ENV__.file}:#{__ENV__.line} #{inspect(result)}")
    {:ok, %{body: body}} = result
    b = body |> Jason.decode!()
    Logger.debug("#{__ENV__.file}:#{__ENV__.line} #{inspect(Map.has_key?(b, "id"))}")
  end

  test "scale an existing actor", context do
    assert is_pid(context[:control_nats])
    host_id = context[:host_id]
    topic = "wasmbus.ctl.#{context[:prefix]}.cmd.#{host_id}.scale"
    actor_id = "MBCFOPM6JW2APJLXJD3Z5O4CN7CPYJ2B4FTKLJUR5YR5MITIU7HD3WD5"
    oci_ref = "wasmcloud.azurecr.io/echo:0.3.2"

    payload =
      %{
        "actor_id" => actor_id,
        "actor_ref" => oci_ref,
        "count" => 1
      }
      |> Jason.encode!()

    result = Gnat.request(context[:control_nats], topic, payload, receive_timeout: 2_000)
    Logger.debug("#{__ENV__.file}:#{__ENV__.line} #{inspect(result)}")
  end

  test "launch actors", context do
    # increases the existing actors count in the user interface. you do not have a name I suppose
    assert is_pid(context[:control_nats])

    host_id = context[:host_id]
    topic = "wasmbus.ctl.#{context[:prefix]}.cmd.#{host_id}.la"

    payload =
      %{
        "actor_ref" => "wasmcloud.azurecr.io/echo:0.3.2",
        "count" => 1
      }
      |> Jason.encode!()

    result = Gnat.request(context[:control_nats], topic, payload, receive_timeout: 2_000)
    Logger.debug("#{__ENV__.file}:#{__ENV__.line} #{inspect(result)}")
    {:ok, %{body: body}} = result
    b = body |> Jason.decode!()
    Logger.debug("#{__ENV__.file}:#{__ENV__.line} #{inspect(Map.has_key?(b, "id"))}")

    # assert true = Map.has_key?(b, "id")
  end

  test "launch actor function", context do
    assert is_pid(context[:control_nats])

    launch_actor(
      context[:host_id],
      context[:prefix],
      "wasmcloud.azurecr.io/echo:0.3.2",
      1,
      context[:control_nats]
    )
  end

  test "launch provider", context do
    assert is_pid(context[:control_nats])

    host_id = context[:host_id]
    topic = "wasmbus.ctl.#{context[:prefix]}.cmd.#{host_id}.lp"

    payload =
      %{
        "provider_ref" => "wasmcloud.azurecr.io/httpserver:0.14.4",
        "link_name" => "default"
      }
      |> Jason.encode!()

    result = Gnat.request(context[:control_nats], topic, payload, receive_timeout: 2_000)
    Logger.debug("#{__ENV__.file}:#{__ENV__.line} #{inspect(result)}")
    {:ok, %{body: body}} = result
    b = body |> Jason.decode!()
    Logger.debug("#{__ENV__.file}:#{__ENV__.line} #{inspect(Map.has_key?(b, "accepted"))}")

    # assert true = Map.has_key?(b, "id")
  end

  test "put link definition", context do
    assert is_pid(context[:control_nats])

    # host_id = context[:host_id]
    topic = "wasmbus.ctl.#{context[:prefix]}.linkdefs.put"
    oci_ref = "wasmcloud.azurecr.io/httpserver:0.14.4"

    payload =
      %{
        "actor_id" => "MBCFOPM6JW2APJLXJD3Z5O4CN7CPYJ2B4FTKLJUR5YR5MITIU7HD3WD5",
        "contract_id" => " 	wasmcloud:contract",
        "link_name" => "default",
        "provider_id" => "VAG3QITQQ2ODAOWB5TTQSDJ53XK3SHBEIFNK4AYJ5RKAX2UNSCAPHA5M",
        "values" => ""
      }
      |> Jason.encode!()

    result = Gnat.request(context[:control_nats], topic, payload, receive_timeout: 2_000)
    Logger.debug("#{__ENV__.file}:#{__ENV__.line} #{inspect(result)}")
    {:ok, %{body: body}} = result
    b = body |> Jason.decode!()
    Logger.debug("#{__ENV__.file}:#{__ENV__.line} #{inspect(Map.has_key?(b, "accepted"))}")
  end

  test "start actor from appspec", context do
    simple_path = "test/fixtures/simple2.yaml"
    {:ok, pass_spec} = Wadm.Model.AppSpec.from_map(YamlElixir.read_from_file!(simple_path))
    Logger.debug("#{__ENV__.file}:#{__ENV__.line} #{inspect(pass_spec)}")

    Enum.each(pass_spec.components, fn c ->
      Logger.debug("#{__ENV__.file}:#{__ENV__.line} #{inspect(c)}")
    end)

    # term = get_in(Enum.at(pass_spec.components, 0), "image")
    # Logger.debug("#{__ENV__.file}:#{__ENV__.line} #{inspect (term)}")

    # use the match system def somefunc(actor = %Wadm.Model.ActorComponent{})
    comp_0 = Enum.at(pass_spec.components, 0)
    Logger.debug("#{__ENV__.file}:#{__ENV__.line} #{inspect(comp_0.image)}")
    Logger.debug("#{__ENV__.file}:#{__ENV__.line} #{inspect(quote do: comp_0)}")

    for %Wadm.Model.ActorComponent{} = actor_component <- pass_spec.components do
      Logger.debug("#{__ENV__.file}:#{__ENV__.line} #{inspect(actor_component.image)}")

      assert actor_component.image == "wasmcloud.azurecr.io/echo:0.3.2"

      for %Wadm.Model.SpreadScaler{} = actor_spread <- actor_component.traits do
        Logger.debug("#{__ENV__.file}:#{__ENV__.line} #{inspect(actor_spread.replicas)}")

        assert actor_spread.replicas == 4

        launch_actor(
          context[:host_id],
          context[:prefix],
          actor_component.image,
          actor_spread.replicas,
          context[:control_nats]
        )
      end

      result =
        LatticeObserver.Observed.EventProcessor.put_actor_instance(
          LatticeObserver.Observed.Lattice.new(),
          "source_host",
          "pk",
          "instance_id",
          "spec",
          "stamp",
          %{}
        )

      Logger.debug("#{__ENV__.file}:#{__ENV__.line} #{inspect(result)}")

      # %LatticeObserver.Observed.Lattice{actors: %{"pk" => %LatticeObserver.Observed.Actor{call_alias: "", capabilities: [], id: "pk", instances: [%LatticeObserver.Observed.Instance{host_id: "source_host", id: "instance_id", revision: 0, spec_id: "spec", version: ""}], issuer: "", name: "unavailable", tags: [], version: nil}}, hosts: %{}, id: "default", instance_tracking: %{"instance_id" => ~U[2022-04-03 21:09:29.201301Z]}, invocation_log: %{}, linkdefs: [], parameters: %LatticeObserver.Observed.Lattice.Parameters{host_status_decay_rate_seconds: 35}, providers: %{}, refmap: %{}}

      # what does this tell you? create a data structure that look like this

      # Traits.reconcile_trait(desired, component, trait, actual)
    end

    # Enum.each(pass_spec.components, fn (c = %{Wadm.Model.ActorComponent}) ->
    #   Logger.debug("#{__ENV__.file}:#{__ENV__.line} #{inspect(c.image)}")
    # end)
    # launch_actor(
    #   context[:host_id], #spread scalar needs to get used here
    #   context[:prefix],
    #   "wasmcloud.azurecr.io/echo:0.3.2",
    #   1,
    #   context[:control_nats]
    # )
  end

  test "start deployment monitor", context do
    simple_path = "test/fixtures/simple2.yaml"
    {:ok, app_spec} = Wadm.Model.AppSpec.from_map(YamlElixir.read_from_file!(simple_path))
    lattice_prefix = "default"

    {{dm_pid, lm_pid}, log} =
      with_log(fn ->
        {dm_pid, lm_pid} =
          Wadm.Deployments.DeploymentMonitor.start_deployment_monitor(app_spec, lattice_prefix)

        Process.sleep(0_100)
        {dm_pid, lm_pid}
      end)

    # Logger.debug("#{__ENV__.file}:#{__ENV__.line} #{inspect(log)}")
    assert log =~ "Starting Deployment Monitor"
    assert log =~ "Starting lattice monitor"
    assert log =~ "Starting NATS subscriber for"

    # when is the genserver pid persisted?
    assert Wadm.Deployments.DeploymentMonitor.lattice_prefix(dm_pid) == lattice_prefix
  end

  test "start the genserver see what it does" do
    simple_path = "test/fixtures/simple2.yaml"
    {:ok, app_spec} = Wadm.Model.AppSpec.from_map(YamlElixir.read_from_file!(simple_path))
    lattice_prefix = "default"

    opts = %{
      app_spec: app_spec,
      lattice_prefix: lattice_prefix,
      key: "child_key(app_spec.name, app_spec.version, lattice_prefix)"
    }

    {:ok, pid} = Wadm.Deployments.DeploymentMonitor.start_link(opts)
    Logger.debug("#{__ENV__.file}:#{__ENV__.line} #{inspect(pid)}")
    assert Wadm.Deployments.DeploymentMonitor.lattice_prefix(pid) == lattice_prefix

    {_, log} =
      with_log(fn ->
        Process.send_after(pid, :decay_lattice, 0)
        Process.sleep(0_100)
      end)

    # Logger.debug("#{__ENV__.file}:#{__ENV__.line} #{inspect(result)}")
    Logger.debug("#{__ENV__.file}:#{__ENV__.line} #{inspect(log)}")

    assert log =~ "Decay tick"

    # force a write of the state to redis

    # why isn't the PID part working? trying to talk to redis that is not running.
    # run in and see what it does.
    # bring back you mnesia part, which was never tested
    # use the bahaviour to mock it out. Do I fix this
  end

  test "trigger a reconcile" do
    simple_path = "test/fixtures/simple2.yaml"
    {:ok, app_spec} = Wadm.Model.AppSpec.from_map(YamlElixir.read_from_file!(simple_path))
    lattice_prefix = "default"

    {{dm_pid, lm_pid}, log} =
      with_log(fn ->
        {dm_pid, lm_pid} =
          Wadm.Deployments.DeploymentMonitor.start_deployment_monitor(app_spec, lattice_prefix)

        Process.sleep(0_100)
        {dm_pid, lm_pid}
      end)

    # Logger.debug("#{__ENV__.file}:#{__ENV__.line} #{inspect(log)}")
    assert log =~ "Starting Deployment Monitor"
    assert log =~ "Starting lattice monitor"
    assert log =~ "Starting NATS subscriber for"

    # when is the genserver pid persisted?
    assert Wadm.Deployments.DeploymentMonitor.lattice_prefix(dm_pid) == lattice_prefix

    start_actor_event =
      # %{
      #   "public_key" => "pk",
      #   "instance_id" => "instance_id",
      #   "annotations" => "%{@appspec => spec}",
      #   "claims" => %{
      #     "name" => "name",
      #     "caps" => ["test", "test2"],
      #     "version" => "1.0",
      #     "revision" => 0,
      #     "tags" => [],
      #     "issuer" => "ATESTxxx"
      #   }
      # }
      # |>
      LatticeObserver.CloudEvent.new(%{},"actor_started", "host")

    {_, log} =
      with_log(fn ->
        Process.send(dm_pid, {:handle_event, start_actor_event},[])
        Process.sleep(0_100)
      end)

    Logger.debug("#{__ENV__.file}:#{__ENV__.line} #{inspect(log)}")
  end

  def launch_actor(host_id, prefix, actor_ref, actor_count, nats_ctl_pid) do
    topic = "wasmbus.ctl.#{prefix}.cmd.#{host_id}.la"

    payload =
      %{
        "actor_ref" => actor_ref,
        "count" => actor_count
      }
      |> Jason.encode!()

    result = Gnat.request(nats_ctl_pid, topic, payload, receive_timeout: 2_000)
    Logger.debug("#{__ENV__.file}:#{__ENV__.line} #{inspect(result)}")

    case result do
      {:ok, %{body: body}} = result ->
        b = body |> Jason.decode!()
        Logger.debug("#{__ENV__.file}:#{__ENV__.line} #{inspect(Map.has_key?(b, "id"))}")

      _ ->
        Logger.debug(
          "#{__ENV__.file}:#{__ENV__.line} #{inspect(result)} Nats and/or OTP are probably not running"
        )
    end
  end
end
