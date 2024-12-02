import asyncio
import WebsocketHedge_PP
import PositionUpload_PP
import ParametersUpdate_PP
import WebsocketConnection_PP

async def main():
    # Update parameters for a specific bot
    manager = ParametersUpdate_PP.BotParamManager()
    manager.update_bot_params("SUI", notional_per_trade=25, ma=10, max_notional=500, default_max_notional=500, std_coeff=1, min_width=0.065)

    # manager.delete_bot_params('MOODENG')
    print("\nDisplaying parameters for all bots:")
    all_params = manager.get_all_bot_params()
    ParametersUpdate_PP.display_all_params(all_params)

    # Position upload
    

if __name__ == "__main__":
    asyncio.run(main())