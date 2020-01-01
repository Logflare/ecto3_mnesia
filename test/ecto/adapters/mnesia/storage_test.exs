defmodule Ecto.Adapters.Mnesia.StorageIntegrationTest do
  use ExUnit.Case, async: false

  alias Ecto.Adapters.Mnesia

  describe "#storage_up" do
    setup do
      nodes = [node()]

      ExUnit.CaptureLog.capture_log fn ->
        :mnesia.stop()
        :mnesia.delete_schema(nodes)
        :mnesia.start()
      end
      {:ok, nodes: nodes}
    end

    test "should write mnesia files", %{nodes: nodes} do
      ExUnit.CaptureLog.capture_log fn ->
        assert Mnesia.storage_up(nodes: nodes) == :ok
        {:ok, %File.Stat{ctime: created}} = File.stat("./Mnesia.nonode@nohost/schema.DAT")
        {:ok, created} = NaiveDateTime.from_erl(created)
        assert created == NaiveDateTime.utc_now() |> NaiveDateTime.truncate(:second)
      end
    end

    test "should return an error if already up", %{nodes: nodes} do
      ExUnit.CaptureLog.capture_log fn ->
        Mnesia.storage_up(nodes: nodes)
        assert Mnesia.storage_up(nodes) == {:error, :already_up}
      end
    end
  end

  describe "#storage_down" do
    setup do
      nodes = [node()]

      ExUnit.CaptureLog.capture_log fn ->
        :mnesia.stop()
        :mnesia.create_schema(nodes)
        :mnesia.start()
      end
      {:ok, nodes: nodes}
    end

    test "should down storage if up", %{nodes: nodes} do
      ExUnit.CaptureLog.capture_log fn ->
        assert Mnesia.storage_down(nodes: nodes) == :ok

        refute File.exists?("./Mnesia.nonode@nohost/schema.DAT")
      end
    end

    test "WARNING : storage_down stil returns :ok if already down", %{nodes: nodes} do
      ExUnit.CaptureLog.capture_log fn ->
        Mnesia.storage_down(nodes: nodes)
        assert Mnesia.storage_down(nodes: nodes) == :ok

        assert File.exists?("./Mnesia.nonode@nohost")
      end
    end
  end

  describe "#storage_status (gives information only about the current node)" do
    setup do
      nodes = [node()]

      {:ok, nodes: nodes}
    end

    test "should be down if storage down", %{nodes: nodes} do
      ExUnit.CaptureLog.capture_log fn ->
        :mnesia.stop()
        :mnesia.delete_schema(nodes)
        :mnesia.start()
      end

      assert Mnesia.storage_status([]) == :down
    end

    test "should be up if started", %{nodes: nodes} do
      ExUnit.CaptureLog.capture_log fn ->
        :mnesia.stop()
        :mnesia.create_schema(nodes)
        :mnesia.start()
      end

      assert Mnesia.storage_status([]) == :up
    end
  end
end
